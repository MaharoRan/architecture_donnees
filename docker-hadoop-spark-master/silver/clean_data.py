# -*- coding: utf-8 -*-

import os
import re
import requests
import pandas as pd
import geopandas as gpd
import pyarrow.parquet as pq

from shapely.geometry import Point
from concurrent.futures import ThreadPoolExecutor, as_completed

# ============================================================
# CONFIG
# ============================================================

RAW_BASE = "../raw"
SILVER_BASE = "../silver/data"

os.makedirs(SILVER_BASE, exist_ok=True)

# Option: activer/désactiver le fallback de géocodage via variable d'environnement
USE_GEOCODE_FALLBACK = (
    os.getenv("USE_GEOCODE_FALLBACK", "true")
    .lower() in ("1", "true", "yes")
)

# Chemin du cache de géocodage (address -> lat/lon)
GEOCODE_CACHE_PATH = f"{SILVER_BASE}/geocode_cache.parquet"

# ============================================================
# SESSION HTTP
# ============================================================

session = requests.Session()

# ============================================================
# GÉOCODAGE
# ============================================================

def geocode_address(address):
    """
    Géocodage via API Adresse Nationale
    """

    if not address or len(str(address).strip()) < 5:
        return (address, None, None)

    try:

        response = session.get(
            "https://api-adresse.data.gouv.fr/search/",
            params={
                "q": address,
                "citycode": "75056",
                "limit": 1
            },
            timeout=5
        )

        if response.status_code == 200:

            data = response.json()

            if data.get("features"):

                coords = (
                    data["features"][0]
                    ["geometry"]
                    ["coordinates"]
                )

                lon = coords[0]
                lat = coords[1]

                return (address, lat, lon)

    except Exception:
        pass

    return (address, None, None)

print("=== Géocodage terminé ===")

# ============================================================
# CACHE GÉOCODAGE
# ============================================================

def load_geocode_cache():
    """Charge un cache local d'adresses géocodées (parquet) en dict."""

    if os.path.exists(GEOCODE_CACHE_PATH):

        try:

            df_cache = pd.read_parquet(
                GEOCODE_CACHE_PATH
            )

            cache = {
                row["address"]: (
                    None if pd.isna(row["lat"])
                    else row["lat"],

                    None if pd.isna(row["lon"])
                    else row["lon"]
                )
                for _, row in df_cache.iterrows()
            }

            return cache

        except Exception:
            return {}

    return {}


def save_geocode_cache(cache):
    """Sauvegarde le cache d'adresses géocodées en parquet."""

    try:

        rows = []

        for addr, (lat, lon) in cache.items():

            rows.append({
                "address": addr,
                "lat": lat,
                "lon": lon
            })

        if rows:

            df_out = pd.DataFrame(rows)

            df_out.to_parquet(
                GEOCODE_CACHE_PATH,
                engine="pyarrow",
                compression="snappy"
            )

    except Exception:
        pass

# ============================================================
# NETTOYAGE COLONNES
# ============================================================

def clean_columns(df):

    cleaned = {}

    for c in df.columns:

        clean_name = re.sub(
            r"[ ,;{}()\n\t=\-]",
            "_",
            c
        )

        cleaned[c] = clean_name

    return df.rename(columns=cleaned)

print("=== Nettoyage des colonnes terminé ===")


def normalize_parquet_types(gdf):
    """Normalise les colonnes avant écriture parquet pour éviter les erreurs Arrow."""

    text_like_columns = {
        "Code_departement",
        "Code_postal",
        "Code_commune",
        "Section",
        "Numero",
        "No_voie",
    }

    for column in gdf.columns:

        if column == "geometry":
            continue

        if column in text_like_columns or gdf[column].dtype == object:
            gdf[column] = gdf[column].astype("string")

    return gdf

# ============================================================
# DVF SILVER
# ============================================================

def process_dvf_silver():

    print("=== Traitement DVF Silver (GeoParquet) ===")

    # --------------------------------------------------------
    # Lecture multi-CSV
    # --------------------------------------------------------

    print("=== Lecture des fichiers CSV ===")

    paths = []

    for year in range(2021, 2026):

        path = f"{RAW_BASE}/ValeursFoncieres-{year}.csv"

        if os.path.exists(path):
            paths.append(path)

    if not paths:

        print("Aucun fichier DVF trouvé")
        return

    dfs = []

    for path in paths:

        print(f"Lecture : {path}")

        df = pd.read_csv(
            path,
            sep=";",
            low_memory=False
        )

        dfs.append(df)

    df = pd.concat(
        dfs,
        ignore_index=True
    )

    print("=== Concaténation terminée ===")

    # --------------------------------------------------------
    # Filtre Paris
    # --------------------------------------------------------

    df = df[
        (
            df["Code departement"]
            .astype(str)
            == "75"
        )
        |
        (
            df["Code postal"]
            .astype(str)
            .str.startswith("75")
        )
    ]

    print("=== Filtre Paris terminé ===")

    # --------------------------------------------------------
    # Dates
    # --------------------------------------------------------

    df["Date mutation formatee"] = pd.to_datetime(
        df["Date mutation"],
        format="%d/%m/%Y",
        errors="coerce"
    )

    df = df[
        (
            df["Date mutation formatee"]
            .dt.year >= 2021
        )
        &
        (
            df["Date mutation formatee"]
            .dt.year <= 2025
        )
    ]

    print("=== Filtre dates terminé ===")

    # --------------------------------------------------------
    # Coordonnées DVF natives
    # --------------------------------------------------------

    if "Latitude" in df.columns:

        df["latitude"] = pd.to_numeric(
            df["Latitude"],
            errors="coerce"
        )

    else:

        df["latitude"] = None

    if "Longitude" in df.columns:

        df["longitude"] = pd.to_numeric(
            df["Longitude"],
            errors="coerce"
        )

    else:

        df["longitude"] = None

    print(
        "Coordonnées déjà présentes :",
        df["latitude"].notna().sum()
    )

    # --------------------------------------------------------
    # Adresse complète
    # --------------------------------------------------------

    df["Adresse_Complete"] = (
        df["No voie"].fillna("").astype(str)
        + " "
        + df["Type de voie"].fillna("").astype(str)
        + " "
        + df["Voie"].fillna("").astype(str)
        + " "
        + df["Code postal"].fillna("").astype(str)
        + " PARIS"
    ).str.strip()

    # --------------------------------------------------------
    # Géocodage fallback
    # --------------------------------------------------------

    missing_mask = (
        df["latitude"].isna()
        |
        df["longitude"].isna()
    )

    print(
        "Lignes sans coordonnées :",
        missing_mask.sum()
    )

    if USE_GEOCODE_FALLBACK and missing_mask.sum() > 0:

        cache = load_geocode_cache()

        unique_addresses = (
            df.loc[
                missing_mask,
                "Adresse_Complete"
            ]
            .dropna()
            .unique()
        )

        print(
            "Adresses uniques à traiter :",
            len(unique_addresses)
        )

        # ----------------------------------------------------
        # Adresses absentes du cache
        # ----------------------------------------------------

        to_geocode = [
            addr
            for addr in unique_addresses
            if addr not in cache
        ]

        print(
            "Nouvelles requêtes BAN :",
            len(to_geocode)
        )

        # ----------------------------------------------------
        # Géocodage parallèle
        # ----------------------------------------------------

        if to_geocode:

            with ThreadPoolExecutor(
                max_workers=20
            ) as executor:

                futures = [
                    executor.submit(
                        geocode_address,
                        addr
                    )
                    for addr in to_geocode
                ]

                for i, future in enumerate(
                    as_completed(futures),
                    start=1
                ):

                    address, lat, lon = (
                        future.result()
                    )

                    cache[address] = (
                        lat,
                        lon
                    )

                    if i % 100 == 0:

                        print(
                            f"Géocodées : "
                            f"{i}/{len(to_geocode)}"
                        )

        # ----------------------------------------------------
        # Sauvegarde cache
        # ----------------------------------------------------

        save_geocode_cache(cache)

        # ----------------------------------------------------
        # Mapping rapide
        # ----------------------------------------------------

        coords_series = df["Adresse_Complete"].map(
            cache
        )

        df["latitude"] = df["latitude"].fillna(
            coords_series.str[0]
        )

        df["longitude"] = df["longitude"].fillna(
            coords_series.str[1]
        )

    print("=== Mapping terminé ===")

    # --------------------------------------------------------
    # Géométrie
    # --------------------------------------------------------

    geometry = [

        Point(lon, lat)

        if pd.notnull(lat)
        and pd.notnull(lon)

        else None

        for lat, lon in zip(
            df["latitude"],
            df["longitude"]
        )
    ]

    gdf = gpd.GeoDataFrame(
        df,
        geometry=geometry,
        crs="EPSG:4326"
    )

    # --------------------------------------------------------
    # Nettoyage colonnes
    # --------------------------------------------------------

    gdf = clean_columns(gdf)

    # --------------------------------------------------------
    # Sauvegarde GeoParquet
    # --------------------------------------------------------

    gdf = normalize_parquet_types(gdf)

    output_path = (
        f"{SILVER_BASE}/"
        "dvf_paris_2021_2025.parquet"
    )

    print(f"Sauvegarde : {output_path}")

    gdf.to_parquet(
        output_path,
        engine="pyarrow",
        compression="snappy"
    )

    print("=== DVF terminé ===")

# ============================================================
# GARES
# ============================================================

def process_gares_silver():

    print("=== Traitement Gares IDF ===")

    try:

        input_path = (
            f"{RAW_BASE}/"
            "emplacement-des-gares-idf.csv"
        )

        df = pd.read_csv(
            input_path,
            sep=";"
        )

        df = clean_columns(df)

        # ----------------------------------------------------
        # Création géométrie si coords présentes
        # ----------------------------------------------------

        if (
            "Geo_Point_2D" in df.columns
            or "geo_point_2d" in df.columns
        ):

            colname = (
                "Geo_Point_2D"
                if "Geo_Point_2D" in df.columns
                else "geo_point_2d"
            )

            lats = []
            lons = []

            for value in df[colname]:

                try:

                    lat, lon = map(
                        float,
                        str(value).split(",")
                    )

                except Exception:

                    lat, lon = None, None

                lats.append(lat)
                lons.append(lon)

            geometry = [

                Point(lon, lat)

                if lat and lon

                else None

                for lat, lon in zip(
                    lats,
                    lons
                )
            ]

            gdf = gpd.GeoDataFrame(
                df,
                geometry=geometry,
                crs="EPSG:4326"
            )

        else:

            gdf = gpd.GeoDataFrame(df)

        output_path = (
            f"{SILVER_BASE}/"
            "emplacement-des-gares-idf.parquet"
        )

        gdf.to_parquet(
            output_path,
            engine="pyarrow",
            compression="snappy"
        )

        print(f"OK : {output_path}")

    except Exception as e:

        print(f"Erreur gares : {e}")

# ============================================================
# PARQUETS ANNEXES
# ============================================================

def process_parquets_silver():

    print("=== Traitement datasets annexes ===")

    datasets = [
        "iris",
        "hopitaux",
        "etablissements-scolaires-ecoles-elementaires",
        "secteurs-scolaires-colleges",
        "secteurs-scolaires-maternelles",
        "les-arbres",
        "ilots-de-fraicheur-espaces-verts-frais",
        "ilots-de-fraicheur-equipements-activites",
        "dans-ma-rue",
        "chantiers-a-paris",
        "que-faire-a-paris-",
        "lieux-de-tournage-a-paris",
        "terrasses-autorisations",
        "comptages-routiers-permanents",
        "comptage-multimodal-comptages",
        "eclairage-public",
        "les_bureaux_de_poste_et_agences_postales_en_idf",
        "liste_des_associations_parisiennes",
        "postes-publics-des-bibliotheques",
        "sanisettesparis",
        "zones-de-caves-inondees-1910",
        "zones-touristiques-internationales",
        "amenagements-cyclables",
        "velib-emplacement-des-stations"
    ]

    for ds in datasets:

        try:

            print(f"Traitement : {ds}")

            input_path = (
                f"{RAW_BASE}/{ds}.parquet"
            )

            # ------------------------------------------------
            # Lecture parquet
            # ------------------------------------------------

            table = pq.read_table(input_path)

            df = table.to_pandas()

            df = clean_columns(df)

            # ------------------------------------------------
            # GeoDataFrame automatique
            # ------------------------------------------------

            gdf = gpd.GeoDataFrame(df)

            output_path = (
                f"{SILVER_BASE}/{ds}.parquet"
            )

            gdf.to_parquet(
                output_path,
                engine="pyarrow",
                compression="snappy"
            )

            print(f"OK : {ds}")

        except Exception as e:

            print(f"Erreur {ds} : {e}")

# ============================================================
# MAIN
# ============================================================

if __name__ == "__main__":

    process_dvf_silver()

    print("=== Etape 1 terminée ===")

    process_gares_silver()

    print("=== Etape 2 terminée ===")

    process_parquets_silver()

    print("=== Pipeline GeoParquet terminé ===")
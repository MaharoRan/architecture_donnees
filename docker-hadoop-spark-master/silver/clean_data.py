# -*- coding: utf-8 -*-

import os
import re
import time
import requests
import pandas as pd
import geopandas as gpd
import pyarrow.parquet as pq

from shapely.geometry import Point

# ============================================================
# CONFIG
# ============================================================

RAW_BASE = "../raw"
SILVER_BASE = "/silver"

os.makedirs(SILVER_BASE, exist_ok=True)

# ============================================================
# GÉOCODAGE
# ============================================================

def geocode_address(address):
    """
    Géocodage via API Adresse Nationale
    """
    if not address or len(str(address).strip()) < 5:
        return (None, None)

    try:
        response = requests.get(
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
                coords = data["features"][0]["geometry"]["coordinates"]

                lon = coords[0]
                lat = coords[1]

                return (lat, lon)

    except Exception:
        pass

    time.sleep(0.1)

    return (None, None)

# ============================================================
# NETTOYAGE COLONNES
# ============================================================

def clean_columns(df):
    cleaned = {}

    for c in df.columns:
        clean_name = re.sub(r"[ ,;{}()\n\t=\-]", "_", c)
        cleaned[c] = clean_name

    return df.rename(columns=cleaned)

# ============================================================
# DVF SILVER
# ============================================================

def process_dvf_silver():

    print("=== Traitement DVF Silver (GeoParquet) ===")

    # --------------------------------------------------------
    # Lecture multi-CSV
    # --------------------------------------------------------

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

    df = pd.concat(dfs, ignore_index=True)

    # --------------------------------------------------------
    # Filtre Paris
    # --------------------------------------------------------

    df = df[
        (
            df["Code departement"].astype(str) == "75"
        )
        |
        (
            df["Code postal"].astype(str).str.startswith("75")
        )
    ]

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
            df["Date mutation formatee"].dt.year >= 2021
        )
        &
        (
            df["Date mutation formatee"].dt.year <= 2025
        )
    ]

    # --------------------------------------------------------
    # Coordonnées
    # --------------------------------------------------------

    if "latitude" not in df.columns:
        df["latitude"] = None

    if "longitude" not in df.columns:
        df["longitude"] = None

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
    )

    # --------------------------------------------------------
    # Géocodage fallback
    # --------------------------------------------------------

    missing_mask = (
        df["latitude"].isna()
        |
        df["longitude"].isna()
    )

    print(f"Lignes à géocoder : {missing_mask.sum()}")

    for idx in df[missing_mask].index:

        address = df.at[idx, "Adresse_Complete"]

        lat, lon = geocode_address(address)

        df.at[idx, "latitude"] = lat
        df.at[idx, "longitude"] = lon

    # --------------------------------------------------------
    # Géométrie GeoPandas
    # --------------------------------------------------------

    geometry = [
        Point(lon, lat)
        if pd.notnull(lat) and pd.notnull(lon)
        else None
        for lat, lon in zip(df["latitude"], df["longitude"])
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

    output_path = f"{SILVER_BASE}/dvf_paris_2021_2025.parquet"

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
                    lat, lon = map(float, str(value).split(","))
                except Exception:
                    lat, lon = None, None

                lats.append(lat)
                lons.append(lon)

            geometry = [
                Point(lon, lat)
                if lat and lon
                else None
                for lat, lon in zip(lats, lons)
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

    process_gares_silver()

    process_parquets_silver()

    print("=== Pipeline GeoParquet terminé ===")
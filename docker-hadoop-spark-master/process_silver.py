# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, to_date, year, concat_ws, lit, coalesce
from pyspark.sql.types import StructType, StructField, DoubleType
import requests
import time

def get_spark_session():
    return SparkSession.builder \
        .appName("SilverLayerProcessing") \
        .getOrCreate()

# Fonction UDF (User Defined Function) pour géocoder une adresse via l'API Adresse publique
def geocode_address(address):
    if not address or len(address.strip()) < 5:
        return (None, None)
    
    try:
        # Appel à l'API publique DVF / Base Adresse Nationale
        response = requests.get(
            "https://api-adresse.data.gouv.fr/search/",
            params={"q": address, "citycode": "75056", "limit": 1}, # 75056 est le code INSEE global de Paris
            timeout=5
        )
        if response.status_code == 200:
            data = response.json()
            if data and "features" in data and len(data["features"]) > 0:
                coords = data["features"][0]["geometry"]["coordinates"]
                return (coords[1], coords[0]) # Retourne (Latitude, Longitude)
    except Exception as e:
        pass
    
    # Petite pause pour ne pas surcharger l'API publique brutalement (attention, ralentit Spark)
    time.sleep(0.1)
    return (None, None)

# Définition du type de retour pour l'UDF
geocode_schema = StructType([
    StructField("lat", DoubleType(), True),
    StructField("lon", DoubleType(), True)
])

# Enregistrement de la fonction en UDF PySpark
geocode_udf = udf(geocode_address, geocode_schema)

def process_dvf_silver(spark):
    print("--- Démarrage du traitement Silver pour DVF ---")

    # 1. Lecture des CSV bruts depuis HDFS (Toutes les années 2021-2025)
    raw_df = spark.read \
        .option("header", "true") \
        .option("sep", ";") \
        .csv("hdfs://namenode:9000/data/ValeursFoncieres-*/ValeursFoncieres-*.csv")

    # 2. Filtre Spatial : Uniquement Paris (75)
    # Les codes départements peuvent être '75'
    df_paris = raw_df.filter(
        (col("Code departement") == "75") | 
        (col("Code postal").startswith("75"))
    )

    # 3. Filtre Temporel : 2021 à 2025
    # Conversion de la date (format typique DVF: dd/MM/yyyy)
    df_paris = df_paris.withColumn("Date mutation formatee", to_date(col("Date mutation"), "dd/MM/yyyy"))
    df_paris = df_paris.filter(
        (year(col("Date mutation formatee")) >= 2021) & 
        (year(col("Date mutation formatee")) <= 2025)
    )

    # 4. Traitement des Coordonnées
    # Construction d'une colonne avec l'adresse complète au cas où les coords manquent
    df_paris = df_paris.withColumn(
        "Adresse_Complete",
        concat_ws(" ", col("No voie"), col("Type de voie"), col("Voie"), col("Code postal"), lit("PARIS"))
    )

    # Séparation : ceux qui ont des coordonnées (si vos CSV en incluent de base)
    # Si le schema de base n'a pas lat/lon, on les crée
    if "latitude" not in df_paris.columns:
        df_paris = df_paris.withColumn("latitude", lit(None).cast(DoubleType()))
        df_paris = df_paris.withColumn("longitude", lit(None).cast(DoubleType()))

    # Pour éviter d'appeler l'API sur 500 000 lignes, on filtre uniquement ceux qui en ont besoin
    df_missing_coords = df_paris.filter(col("latitude").isNull() | col("longitude").isNull())
    df_has_coords = df_paris.filter(col("latitude").isNotNull() & col("longitude").isNotNull())

    print(f"Lignes nécessitant un fallback de géocodage par API: {df_missing_coords.count()}")

    # Géocodage
    df_geocoded = df_missing_coords.withColumn("coords", geocode_udf(col("Adresse_Complete")))
    df_geocoded = df_geocoded.withColumn("latitude", coalesce(col("latitude"), col("coords.lat"))) \
                             .withColumn("longitude", coalesce(col("longitude"), col("coords.lon"))) \
                             .drop("coords", "Adresse_Complete")

    df_has_coords = df_has_coords.drop("Adresse_Complete")
    
    # 5. Union finale des données propres
    df_silver_final = df_has_coords.unionByName(df_geocoded)

    # Nettoyage des noms de colonnes pour le format Parquet (qui n'accepte pas les espaces etc.)
    import re
    for c in df_silver_final.columns:
        clean_name = re.sub(r'[ ,;{}()\n\t=]', '_', c)
        if clean_name != c:
            df_silver_final = df_silver_final.withColumnRenamed(c, clean_name)

    # 6. Sauvegarde dans la couche Silver (en Parquet pour des performances maximales)
    silver_path = "hdfs://namenode:9000/data/silver/dvf_paris_2021_2025.parquet"
    print(f"Sauvegarde des données Silver dans : {silver_path}")
    
    df_silver_final.write \
        .mode("overwrite") \
        .parquet(silver_path)

    print("--- Fin du traitement Silver DVF ---")

def process_gares_silver(spark):
    print("\n--- Démarrage du traitement Silver pour les gares IDF (CSV) ---")
    try:
        # Le fichier est un CSV séparé par des points-virgules
        # Feeder.py le place dans son propre sous-dossier
        input_path = "hdfs://namenode:9000/data/emplacement-des-gares-idf/emplacement-des-gares-idf.csv"
        df = spark.read.option("header", "true").option("sep", ";").csv(input_path)
        
        # Nettoyage des noms de colonnes pour Parquet
        import re
        for c in df.columns:
            clean_name = re.sub(r'[ ,;{}()\n\t=-]', '_', c)
            if clean_name != c:
                df = df.withColumnRenamed(c, clean_name)
        
        # Sauvegarde en format Silver (Propre)
        output_path = "hdfs://namenode:9000/data/silver/emplacement-des-gares-idf.parquet"
        df.write.mode("overwrite").parquet(output_path)
        print(f" -> OK: Gares sauvegardées dans {output_path}")
        
    except Exception as e:
        print(f" -> ATTENTION: Impossible de traiter emplacement-des-gares-idf (Ignoré) : {str(e)[:100]}...")

def process_parquets_silver(spark):
    print("\n--- Démarrage du traitement Silver pour les datasets annexes (Parquet) ---")
    import re
    
    # Liste exhaustive de tous les datasets Parquet présents dans la source
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
            print(f"Traitement de {ds}...")
            # On cherche le fichier là où feeder.py l'a rangé
            input_path = f"hdfs://namenode:9000/data/{ds}/{ds}.parquet"
            df = spark.read.parquet(input_path)
            
            # Nettoyage strict des noms de colonnes pour le format Parquet
            # On remplace aussi les tirets par des underscores
            for c in df.columns:
                clean_name = re.sub(r'[ ,;{}()\n\t=-]', '_', c)
                if clean_name != c:
                    df = df.withColumnRenamed(c, clean_name)
            
            # Sauvegarde en format Silver (Propre)
            output_path = f"hdfs://namenode:9000/data/silver/{ds}.parquet"
            df.write.mode("overwrite").parquet(output_path)
            print(f" -> OK: Sauvegardé dans {output_path}")
            
        except Exception as e:
            print(f" -> ATTENTION: Impossible de traiter {ds} (Ignoré) : {str(e)[:100]}...")

if __name__ == "__main__":
    spark = get_spark_session()
    
    # 1. Traitement massif des transactions immobilières DVF (CSV)
    process_dvf_silver(spark)
    
    # 2. Traitement des gares (CSV additionnel)
    process_gares_silver(spark)
    
    # 3. Traitement et nettoyage des référentiels (Parquet)
    process_parquets_silver(spark)
    
    spark.stop()
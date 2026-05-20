# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, split

def get_spark_session():
    return SparkSession.builder \
        .appName("GoldLayerDatamarts") \
        .getOrCreate()

def extract_poi(spark, dataset_name, categorie_name):
    """
    Fonction utilitaire pour extraire les coordonnées géographiques d'un dataset
    Silver de l'Open Data Paris et standardiser le format pour notre Datamart.
    """
    try:
        input_path = f"hdfs://namenode:9000/data/silver/{dataset_name}.parquet"
        df = spark.read.parquet(input_path)
        
        cols_lower = [c.lower() for c in df.columns]
        
        # Beaucoup de datasets de la ville de Paris utilisent `Geo_Point` ou `geo_point_2d` ou `Geo_Shape`
        geo_col = None
        for candidate in ["geo_point", "geo_point_2d", "coordonnees"]:
            if candidate in cols_lower:
                idx = cols_lower.index(candidate)
                geo_col = df.columns[idx]
                break
                
        if geo_col:
            # On sépare la chaîne "lat, lon"
            df = df.withColumn("latitude", split(col(geo_col), ",")[0].cast("double"))
            df = df.withColumn("longitude", split(col(geo_col), ",")[1].cast("double"))
        elif "latitude" in cols_lower and "longitude" in cols_lower:
            df = df.withColumn("latitude", col("latitude").cast("double"))
            df = df.withColumn("longitude", col("longitude").cast("double"))
        else:
            print(f" [!] Coordonnées introuvables pour {dataset_name}. Colonnes dispo: {df.columns[:5]}...")
            return None
            
        print(f" [+] Extrait {dataset_name} en tant que: {categorie_name}")
        return df.select(
            col("latitude"),
            col("longitude"),
            lit(categorie_name).alias("categorie")
        ).filter(col("latitude").isNotNull() & col("longitude").isNotNull())
        
    except Exception as e:
        print(f" [x] Erreur ou fichier manquant pour {dataset_name}: {str(e).split(';')[0]}")
        return None

def build_datamarts(spark):
    print("=== CRÉATION DES DATAMARTS GOLD (POUR CARTE INTERACTIVE) ===")

    # ==========================================
    # DATAMART 0 : Immobilier (La base de la carte)
    # ==========================================
    print("\n--- Construction Datamart 0: Valeurs Foncières ---")
    try:
        df_immo = spark.read.parquet("hdfs://namenode:9000/data/silver/dvf_paris_2021_2025.parquet")
        # On ne garde que les infos essentielles pour alléger l'affichage de la carte
        # Remplacement des espaces traités dans Silver par des underscores _
        df_immo_gold = df_immo.select(
            col("Valeur_fonciere").cast("double").alias("prix"),
            col("Surface_reelle_bati").cast("double").alias("surface"),
            col("Type_local").alias("type_bien"),
            # On s'assure de prendre la lat/lon propre générée dans Silver
            col("latitude").cast("double").alias("latitude"),
            col("longitude").cast("double").alias("longitude")
        ).filter(col("latitude").isNotNull() & col("prix").isNotNull())
        
        df_immo_gold.write.mode("overwrite").parquet("hdfs://namenode:9000/data/gold/dm_immobilier.parquet")
        print(" -> DM Immobilier Sauvegardé")
    except Exception as e:
        print("Erreur DVF:", e)

    # ==========================================
    # DATAMART 1 : Qualité de vie & Tranquillité
    # ==========================================
    print("\n--- Construction Datamart 1: Qualité de vie ---")
    qdv_sources = [
        ("ilots-de-fraicheur-espaces-verts-frais", "Espace vert & Frais"),
        ("les-arbres", "Arbre"),
        ("ilots-de-fraicheur-equipements-activites", "Equipement Fraicheur"),
        ("eclairage-public", "Eclairage Public"),
        ("sanisettesparis", "Toilettes"),
        ("comptages-routiers-permanents", "Trafic Routier"),
        ("chantiers-a-paris", "Chantier en cours"),
        ("dans-ma-rue", "Anomalie/Incivilité signalée"),
        ("zones-touristiques-internationales", "Zone Touristique"),
        ("terrasses-autorisations", "Terrasse Commerciale"),
        ("zones-de-caves-inondees-1910", "Risque Inondation")
    ]
    df_qdv = None
    for src, cat in qdv_sources:
        df_tmp = extract_poi(spark, src, cat)
        if df_tmp:
            df_qdv = df_tmp if df_qdv is None else df_qdv.unionByName(df_tmp)
    if df_qdv:
        df_qdv.write.mode("overwrite").parquet("hdfs://namenode:9000/data/gold/dm_qualite_vie.parquet")
        print(" -> DM Qualité de vie Sauvegardé")

    # ==========================================
    # DATAMART 2 : Culture & Loisirs
    # ==========================================
    print("\n--- Construction Datamart 2: Culture & Loisir ---")
    culture_sources = [
        ("que-faire-a-paris-", "Activité & Evénement"),
        ("liste_des_associations_parisiennes", "Association"),
        ("lieux-de-tournage-a-paris", "Lieu de tournage"),
        ("ilots-de-fraicheur-espaces-verts-frais", "Espace vert & Parc")
    ]
    df_cult = None
    for src, cat in culture_sources:
        df_tmp = extract_poi(spark, src, cat)
        if df_tmp:
            df_cult = df_tmp if df_cult is None else df_cult.unionByName(df_tmp)
    if df_cult:
        df_cult.write.mode("overwrite").parquet("hdfs://namenode:9000/data/gold/dm_culture_loisir.parquet")
        print(" -> DM Culture Sauvegardé")

    # ==========================================
    # DATAMART 3 : Accès Services et Commerces
    # ==========================================
    print("\n--- Construction Datamart 3: Services & Commerces ---")
    service_sources = [
        ("secteurs-scolaires-colleges", "Collège"),
        ("etablissements-scolaires-ecoles-elementaires", "Ecole Elémentaire"),
        ("secteurs-scolaires-maternelles", "Ecole Maternelle"),
        ("postes-publics-des-bibliotheques", "Bibliothèque"),
        ("hopitaux", "Hôpital"),
        ("les_bureaux_de_poste_et_agences_postales_en_idf", "Bureau de Poste")
        # Note: Pour les Commissariats et Pharmacie, s'ils ne sont pas traités dans Silver, ils seront ignorés pour l'instant.
    ]
    df_serv = None
    for src, cat in service_sources:
        df_tmp = extract_poi(spark, src, cat)
        if df_tmp:
            df_serv = df_tmp if df_serv is None else df_serv.unionByName(df_tmp)
    if df_serv:
        df_serv.write.mode("overwrite").parquet("hdfs://namenode:9000/data/gold/dm_services_commerces.parquet")
        print(" -> DM Services Sauvegardé")

    # ==========================================
    # DATAMART 4 : Transport
    # ==========================================
    print("\n--- Construction Datamart 4: Transport ---")
    transport_sources = [
        ("velib-emplacement-des-stations", "Station Vélib"),
        ("amenagements-cyclables", "Aménagement Cyclable"),
        ("emplacement-des-gares-idf", "Gare / Station")
    ]
    df_trans = None
    for src, cat in transport_sources:
        df_tmp = extract_poi(spark, src, cat)
        if df_tmp:
            df_trans = df_tmp if df_trans is None else df_trans.unionByName(df_tmp)
    if df_trans:
        df_trans.write.mode("overwrite").parquet("hdfs://namenode:9000/data/gold/dm_transport.parquet")
        print(" -> DM Transport Sauvegardé")

if __name__ == "__main__":
    spark = get_spark_session()
    build_datamarts(spark)
    spark.stop()
    print("=== TERMINÉ ===")
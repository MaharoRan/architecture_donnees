import geopandas as gpd
from sqlalchemy import create_engine

def load_to_postgis(input_path, db_uri, table_name):
    # Load cleaned data
    gdf = gpd.read_parquet(input_path)

    # Connect to PostgreSQL
    engine = create_engine(db_uri)

    # Write to PostGIS table
    gdf.to_postgis(table_name, engine, if_exists='replace')
    print(f"Data loaded into PostgreSQL table: {table_name}")

# Example usage
load_to_postgis(
    "silver/data_cleaned.parquet",
    "postgresql://user:password@localhost:5432/mydatabase",
    "cleaned_data"
)
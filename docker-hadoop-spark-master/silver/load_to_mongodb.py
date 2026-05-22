from pymongo import MongoClient
import geopandas as gpd
import os

mongo_user = os.getenv("MONGO_USER")
mongo_password = os.getenv("MONGO_PASSWORD")

def load_to_mongodb(input_path, mongo_uri, database, collection):
    # Load cleaned data
    gdf = gpd.read_parquet(input_path)

    # Convert to GeoJSON
    geojson = gdf.to_json()

    # Connect to MongoDB
    client = MongoClient(mongo_uri)
    db = client[database]
    col = db[collection]

    # Insert GeoJSON data
    col.insert_many(gdf.to_dict('records'))
    print(f"Data loaded into MongoDB collection: {collection}")

if __name__ == "__main__":
    load_to_mongodb(
        "iris.parquet",
        f"mongodb+srv://{mongo_user}:{mongo_password}@cluster0.3bidwmj.mongodb.net/",
        "dataarchi",
        "location"
    )

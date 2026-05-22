from pathlib import Path
import os
from urllib.parse import quote_plus

import pandas as pd
from pymongo import MongoClient
from shapely.geometry import mapping
from shapely import wkb

BASE_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = BASE_DIR.parent
ENV_PATH = PROJECT_ROOT / ".env"


def _load_env_file(env_path):
    if not env_path.exists():
        return

    with env_path.open("r", encoding="utf-8") as env_file:
        for raw_line in env_file:
            line = raw_line.strip()

            if not line or line.startswith("#") or "=" not in line:
                continue

            key, value = line.split("=", 1)
            key = key.strip()
            value = value.strip().strip('"').strip("'")

            if key and key not in os.environ:
                os.environ[key] = value


_load_env_file(ENV_PATH)


def _get_mongo_credentials():
    user = os.getenv("MONGO_USER")
    password = os.getenv("MONGO_PASSWORD")

    if not user or not password:
        raise RuntimeError(
            "Variables MongoDB manquantes. Vérifie le fichier .env ou les variables d'environnement MONGO_USER et MONGO_PASSWORD."
        )

    return user, password


def _build_mongo_uri(host, user, password):
    return f"mongodb+srv://{quote_plus(user)}:{quote_plus(password)}@{host}/?retryWrites=true&w=majority"


def _resolve_input_path(input_path):
    path = Path(input_path)

    if path.is_absolute():
        return path

    candidate = BASE_DIR / path
    if candidate.exists():
        return candidate

    candidate = BASE_DIR / "data" / path
    if candidate.exists():
        return candidate

    return BASE_DIR / "data" / path.name


def _to_geojson(value):
    if value is None:
        return None

    try:
        if pd.isna(value):
            return None
    except Exception:
        pass

    if isinstance(value, (bytes, bytearray, memoryview)):
        return mapping(wkb.loads(bytes(value)))

    return value


def _prepare_records(df):
    records = df.to_dict("records")

    for record in records:
        if "geo_point_2d" in record:
            record["location"] = _to_geojson(record["geo_point_2d"])

        if "geo_shape" in record:
            record["geometry"] = _to_geojson(record["geo_shape"])

        record.pop("geo_point_2d", None)
        record.pop("geo_shape", None)

    return records

def load_to_mongodb(input_path, mongo_uri, database, collection):
    resolved_path = _resolve_input_path(input_path)

    if not resolved_path.exists():
        raise FileNotFoundError(f"Parquet introuvable: {resolved_path}")

    # Lecture robuste sans dépendre des métadonnées GeoParquet
    df = pd.read_parquet(resolved_path)

    # Préparer des documents MongoDB sérialisables
    records = _prepare_records(df)

    # Connect to MongoDB
    client = MongoClient(mongo_uri)
    db = client[database]
    col = db[collection]

    # Insert documents
    if records:
        col.insert_many(records)

    print(f"Data loaded into MongoDB collection: {collection}")

if __name__ == "__main__":
    mongo_user, mongo_password = _get_mongo_credentials()

    load_to_mongodb(
        "data/iris.parquet",
        _build_mongo_uri(
            "cluster0.3bidwmj.mongodb.net",
            mongo_user,
            mongo_password,
        ),
        "dataarchi",
        "location"
    )

import os
import json
import glob
import shutil
import requests
import pandas as pd

# config
API_URL = "https://opendata.paris.fr/api/explore/v2.1/catalog/datasets/comptages-routiers-permanents/records?limit=100"

BRONZE_DIR = "./data/bronze/trafic"
ARCHIVE_DIR = "./data/archive/trafic"


def create_folders():
    os.makedirs(BRONZE_DIR, exist_ok=True)
    os.makedirs(ARCHIVE_DIR, exist_ok=True)


def get_timestamp():
    return pd.Timestamp.now().strftime("%Y-%m-%d_%H%M%S")

# déplace les fichier ancien dans archive
def archive_old_bronze_files():
    files = glob.glob(os.path.join(BRONZE_DIR, "*.json"))

    if not files:
        print("Aucun ancien fichier Bronze à archiver.")
        return

    for file_path in files:
        file_name = os.path.basename(file_path)
        archived_name = f"archived_{get_timestamp()}_{file_name}"
        destination = os.path.join(ARCHIVE_DIR, archived_name)

        shutil.move(file_path, destination)
        print(f"Fichier archivé : {destination}")


# récupere les données brut api
def ingest_to_bronze():
    create_folders()
    archive_old_bronze_files()

    try:
        print("Téléchargement des données depuis l'API...")
        response = requests.get(API_URL, timeout=30)
        response.raise_for_status()

        data = response.json()

        filename = f"raw_trafic_{get_timestamp()}.json"
        filepath = os.path.join(BRONZE_DIR, filename)

        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=4)

        print(f"Nouveau fichier Bronze enregistré dans : {filepath}")

    except requests.exceptions.RequestException as e:
        print(f"Erreur API : {e}")


if __name__ == "__main__":
    ingest_to_bronze()
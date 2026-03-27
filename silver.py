import os
import glob
import json
import pandas as pd
import numpy as np

BRONZE_DIR = "./data/bronze/trafic"
SILVER_DIR = "./data/silver/trafic"
QUARANTINE_DIR = "./data/quarantine/trafic"


def create_folders():
    os.makedirs(SILVER_DIR, exist_ok=True)
    os.makedirs(QUARANTINE_DIR, exist_ok=True)


def get_latest_bronze_file():
    files = glob.glob(os.path.join(BRONZE_DIR, "*.json"))
    if not files:
        return None
    return max(files, key=os.path.getctime)


def get_timestamp():
    return pd.Timestamp.now().strftime("%Y-%m-%d_%H%M%S")


def load_bronze_data(filepath):
    with open(filepath, "r", encoding="utf-8") as f:
        return json.load(f)


def clean_trafic_data(raw_data):
    results = raw_data.get("results", raw_data)

    if not results:
        return pd.DataFrame()

    df = pd.json_normalize(results)

    cols_map = {
        "iu_ac": "id_arc",
        "libelle": "nom_route",
        "t_1h": "timestamp",
        "q": "debit_vehicules",
        "k": "taux_occupation",
        "etat_trafic": "etat_trafic",
        "geo_point_2d.lat": "latitude",
        "geo_point_2d.lon": "longitude"
    }

    existing_cols = [c for c in cols_map if c in df.columns]
    df = df[existing_cols].rename(columns=cols_map)

    return df


def validate_data(df):
    if df.empty:
        return pd.DataFrame(), pd.DataFrame()

    df = df.copy()
    df["motif_rejet"] = ""

    required_columns = ["id_arc", "timestamp", "debit_vehicules"]
    for col in required_columns:
        if col not in df.columns:
            print(f"Colonne obligatoire absente : {col}")
            return pd.DataFrame(), df

    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
    df["debit_vehicules"] = pd.to_numeric(df["debit_vehicules"], errors="coerce")

    if "taux_occupation" in df.columns:
        df["taux_occupation"] = pd.to_numeric(df["taux_occupation"], errors="coerce")

    df.loc[df["id_arc"].isna(), "motif_rejet"] += "id_arc_manquant; "
    df.loc[df["timestamp"].isna(), "motif_rejet"] += "timestamp_invalide; "
    df.loc[df["debit_vehicules"].isna(), "motif_rejet"] += "debit_invalide; "
    df.loc[df["debit_vehicules"] < 0, "motif_rejet"] += "debit_negatif; "
    df.loc[df["debit_vehicules"] > 5000, "motif_rejet"] += "debit_aberrant; "

    if "taux_occupation" in df.columns:
        df.loc[df["taux_occupation"] < 0, "motif_rejet"] += "taux_negatif; "
        df.loc[df["taux_occupation"] > 100, "motif_rejet"] += "taux_superieur_100; "

    duplicated_mask = df.duplicated(subset=["id_arc", "timestamp"], keep="first")
    df.loc[duplicated_mask, "motif_rejet"] += "doublon; "

    if "etat_trafic" in df.columns:
        df["etat_trafic"] = df["etat_trafic"].replace("Inconnu", np.nan)

    valid_df = df[df["motif_rejet"] == ""].copy()
    rejected_df = df[df["motif_rejet"] != ""].copy()

    return valid_df, rejected_df


def save_outputs(valid_df, rejected_df):
    timestamp = get_timestamp()

    if not valid_df.empty:
        silver_path = os.path.join(SILVER_DIR, f"silver_trafic_{timestamp}.parquet")
        valid_df.to_parquet(silver_path, index=False)
        print(f"Données valides enregistrées dans : {silver_path}")
    else:
        print("Aucune donnée valide à enregistrer en Silver.")

    if not rejected_df.empty:
        quarantine_path = os.path.join(QUARANTINE_DIR, f"quarantine_trafic_{timestamp}.parquet")
        rejected_df.to_parquet(quarantine_path, index=False)
        print(f"Données rejetées enregistrées dans : {quarantine_path}")
    else:
        print("Aucune donnée rejetée.")


def bronze_to_silver():
    create_folders()

    latest_file = get_latest_bronze_file()
    if not latest_file:
        print("Aucun fichier Bronze trouvé.")
        return

    print(f"Lecture du fichier Bronze : {latest_file}")

    raw_data = load_bronze_data(latest_file)
    df = clean_trafic_data(raw_data)

    valid_df, rejected_df = validate_data(df)
    save_outputs(valid_df, rejected_df)


if __name__ == "__main__":
    bronze_to_silver()
import os
import glob
import pandas as pd

SILVER_DIR = "./data/silver/trafic"
GOLD_DIR = "./data/gold/trafic"


def create_folder():
    os.makedirs(GOLD_DIR, exist_ok=True)


def get_latest_silver_file():
    files = glob.glob(os.path.join(SILVER_DIR, "*.parquet"))
    if not files:
        return None
    return max(files, key=os.path.getctime)


def get_timestamp():
    return pd.Timestamp.now().strftime("%Y-%m-%d_%H%M%S")


def silver_to_gold():
    create_folder()

    latest_file = get_latest_silver_file()
    if not latest_file:
        print("Aucun fichier Silver trouvé.")
        return

    print(f"Lecture du fichier Silver : {latest_file}")

    df = pd.read_parquet(latest_file)

    if df.empty:
        print("Le fichier Silver est vide.")
        return

    group_cols = ["id_arc"]
    if "nom_route" in df.columns:
        group_cols.append("nom_route")

    agg_dict = {"debit_vehicules": "mean"}

    if "taux_occupation" in df.columns:
        agg_dict["taux_occupation"] = "mean"

    df_gold = df.groupby(group_cols, dropna=False).agg(agg_dict).reset_index()

    df_gold = df_gold.rename(columns={
        "debit_vehicules": "debit_moyen",
        "taux_occupation": "taux_occupation_moyen"
    })

    output_path = os.path.join(GOLD_DIR, f"gold_trafic_{get_timestamp()}.parquet")
    df_gold.to_parquet(output_path, index=False)

    print(f"Données Gold enregistrées dans : {output_path}")


if __name__ == "__main__":
    silver_to_gold()
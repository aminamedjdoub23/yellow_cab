import pandas as pd
import glob

# script pour convertir les parquets 2024 car hadoop plante
# erreur : native zStandard library not available
# on repasse tout en snappy pour que spark puisse les lire

path = '../source/nyc_yellow_taxi_dataset/*.parquet'
files = glob.glob(path)

print("debut conversion")

for f in files:
    if "_snappy" not in f:
        print("fichier en cours :", f)
        
        df = pd.read_parquet(f)
        
        # rename pour pas ecraser l'original
        new_f = f.replace(".parquet", "_snappy.parquet")
        
        df.to_parquet(new_f, compression="snappy")
        print("ok")

print("fin de la conversion")

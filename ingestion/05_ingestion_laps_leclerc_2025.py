import requests
import pandas as pd
from sqlalchemy import create_engine

DB_URL = "postgresql://f1_user:f1_password@localhost:5432/f1_db"

def ingest_laps(driver_number, date_start, date_end):

    url = f"https://api.openf1.org/v1/laps"
    params = {"driver_number": driver_number, "date_start>": date_start, "date_start<": date_end}
    
    print(requests.get(url, params=params).url)  # Affiche l'URL complète pour le débogage
    response = requests.get(url, params=params)
    print(f"📡 Recupération des tours pour le pilote numéro {driver_number} sur l'année de {date_start}")    

    if response.status_code == 200:
        
        data = response.json()
        df = pd.DataFrame(data)
        f"✅ {len(data)} laps récupérés"
        engine = create_engine(DB_URL)

        df.to_sql("raw_laps_leclerc_2025", engine, if_exists="replace", index=False)
        print(f"💾 Sauvegardé dans raw_laps_leclerc_2025 avec {len(df)} lignes") 

        return df
    else:
        print(f"❌ API error: {response.status_code}")
        return None

if __name__ == "__main__":
    DRIVER_NUMBER = 16
    DATE_START = "2025-01-01"
    DATE_END = "2025-12-31"
    ingest_laps(DRIVER_NUMBER, DATE_START, DATE_END)
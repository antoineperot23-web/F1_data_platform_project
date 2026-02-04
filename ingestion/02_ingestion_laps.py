import requests
import pandas as pd
from sqlalchemy import create_engine

DB_URL = "postgresql://f1_user:f1_password@localhost:5432/f1_db"

def ingest_laps(session_key):
    # 1. URL OpenF1 laps + ton session_key
    url = f"https://api.openf1.org/v1/laps"
    params = {"session_key": session_key}
    
    response = requests.get(url, params=params)
    print(f"📡 Recupération des tours pour session_key={session_key}")    

    if response.status_code == 200:
        
        data = response.json()
        df = pd.DataFrame(data)
        f"✅ {len(data)} laps récupérés"
        engine = create_engine(DB_URL)

        df.to_sql("raw_laps", engine, if_exists="replace", index=False)
        print(f"💾 Sauvegardé dans raw_laps avec {len(df)} lignes") 

        return df
    else:
        print(f"❌ API error: {response.status_code}")
        return None

if __name__ == "__main__":
    SESSION_KEY = "9939"
    ingest_laps(SESSION_KEY)

import requests
import pandas as pd
from sqlalchemy import create_engine
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

DB_URL = "postgresql://f1_user:f1_password@localhost:5432/f1_db"

def ingest_sessions():
    url = f"https://api.openf1.org/v1/sessions"
    
    logger.info(f"📡 Récupération sessions ...")
    response = requests.get(url)
    
    if response.status_code == 200:
        data = response.json()
        logger.info(f"✅ {len(data)} sessions")
        
        df = pd.DataFrame(data)
        engine = create_engine(DB_URL)
        
        df.to_sql("raw_sessions", engine, if_exists="replace", index=False)
        logger.info("💾 Sauvegardé dans raw_sessions")
        
        # Print colonnes disponibles + aperçu
        print("\n📋 Colonnes disponibles:", df.columns.tolist())
        print("\n📊 Aperçu (5 premières lignes):")
        print(df.head())
        
        return df
    else:
        logger.error(f"❌ API error: {response.status_code}")
        return None

if __name__ == "__main__":
    ingest_sessions()

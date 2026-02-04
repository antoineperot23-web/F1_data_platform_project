import pandas as pd
from sqlalchemy import create_engine

DB_URL = "postgresql://f1_user:f1_password@localhost:5432/f1_db"

def transform_laps_to_stats():
    engine = create_engine(DB_URL)
    
    # 1. Lire raw_laps
    df_laps = pd.read_sql("SELECT * FROM raw_laps", engine)
    
    # 2. Nettoyer : supprimer NaN sur lap_duration
    df_clean = df_laps.dropna(subset=['lap_duration'])
    
    # 3. Grouper par pilote : moyenne, min, max
    stats = df_clean.groupby('driver_number').agg({
        'lap_duration': ['mean', 'min', 'max', 'count']
    }).round(3)
    
    # 4. Renommer colonnes
    stats.columns = ['avg_lap', 'best_lap', 'worst_lap', 'laps_count']
    
    # 5. Sauvegarder
    stats.to_sql("driver_lap_stats", engine, if_exists="replace")
    
    print("📊 Stats pilotes:")
    print(stats.sort_values('best_lap'))
    
    return stats

if __name__ == "__main__":
    transform_laps_to_stats()

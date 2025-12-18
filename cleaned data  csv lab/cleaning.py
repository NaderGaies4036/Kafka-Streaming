import pandas as pd
import re
from datetime import datetime

def clean_transactions(input_file, output_file):
    
    print("Nettoyage des données en cours...\n")    
    df = pd.read_csv(input_file)
    
    print(f"Données initiales: {len(df)} lignes")
    print(f"Colonnes: {list(df.columns)}\n")
    
    print(" PROBLÈMES DÉTECTÉS:")
    print(f"- Valeurs manquantes dans 'amount': {df['amount'].isna().sum()}")
    print(f"- Valeurs manquantes dans 'timestamp': {df['timestamp'].isna().sum()}")
    print(f"- Lignes incomplètes: {df.isna().all(axis=1).sum()}")
    print(f"- Doublons: {df.duplicated().sum()}\n")
    
    df_clean = df.dropna(how='all')
    print(f" Lignes vides supprimées: {len(df) - len(df_clean)}")
    
    before_dup = len(df_clean)
    df_clean = df_clean.drop_duplicates()
    print(f"✓ Doublons supprimés: {before_dup - len(df_clean)}")
    
    def clean_user_id(value):
        try:
            return int(value)
        except (ValueError, TypeError):
            return None
    
    df_clean['user_id'] = df_clean['user_id'].apply(clean_user_id)
    invalid_users = df_clean['user_id'].isna().sum()
    df_clean = df_clean.dropna(subset=['user_id'])
    print(f" user_id invalides supprimés: {invalid_users}")
    
    def clean_amount(value):
        try:
            return float(value)
        except (ValueError, TypeError):
            return None
    
    df_clean['amount'] = df_clean['amount'].apply(clean_amount)
    invalid_amounts = df_clean['amount'].isna().sum()
    df_clean = df_clean.dropna(subset=['amount'])
    print(f"✓ amount invalides supprimés: {invalid_amounts}")
    
    def clean_timestamp(value):
        if pd.isna(value):
            return None
        
        formats = [
            '%Y-%m-%d %H:%M:%S',
            '%Y/%m/%d %H:%M:%S',
        ]
        
        for fmt in formats:
            try:
                return datetime.strptime(str(value), fmt).strftime('%Y-%m-%d %H:%M:%S')
            except ValueError:
                continue
        
        return None
    
    df_clean['timestamp'] = df_clean['timestamp'].apply(clean_timestamp)
    invalid_timestamps = df_clean['timestamp'].isna().sum()
    df_clean = df_clean.dropna(subset=['timestamp'])
    print(f"✓ timestamp invalides supprimés: {invalid_timestamps}")
    
    df_clean['transaction_id'] = df_clean['transaction_id'].astype(int)
    df_clean['user_id'] = df_clean['user_id'].astype(int)
    df_clean['amount'] = df_clean['amount'].astype(float)
    
    df_clean.to_csv(output_file, index=False)
    
    print(f"\n NETTOYAGE TERMINÉ")
    print(f" Données finales: {len(df_clean)} lignes (nettoyées)")
    print(f" Fichier sauvegardé: {output_file}")

    return df_clean

if __name__ == "__main__":
    df_clean = clean_transactions(
        "transactions_dirty.csv",
        "transactions_clean.csv"
    )
    

    print("\n📋 APERÇU DES DONNÉES NETTOYÉES:")
    print(df_clean.head(10))
# Imports
from datetime import date
from random import randint, choice
from datetime import datetime
import os
import clickhouse_connect
import pandas as pd
import clickhouse_connect
from pymongo import MongoClient
import pandas as pd
import os
import lakehouseConfig as lakehouseConfig
import mongo as mng

# Ruta donde está el CSV de logs (ajusta según tu carpeta)
ruta_data = r'C:\Users\pablo\Desktop\Master\GestionAlmacenamientoBigData\PracticaFinal\data'
path_logs_csv = os.path.join(ruta_data, 'logs_web.csv')

def ingest_bronze():
    ch_client = lakehouseConfig.get_client()
    mongo_client, mongo_db = mng.create_mongo_connection()
    
    print("🚀 Iniciando ingesta a Capa BRONZE...")

    # ---------------------------------------------------------
    # 1. INGESTA LOGS (CSV) -> ClickHouse (bronze.logs_web)
    #  "logs_web.csv" -> Ingestar como fichero CSV.
    # ---------------------------------------------------------
    try:
        if os.path.exists(path_logs_csv):
            print(f"   Leyendo CSV: {path_logs_csv}...")
            # Leemos todo como string (dtype=str) para cumplir con la tabla Bronze definida
            df_logs = pd.read_csv(path_logs_csv, dtype=str)
            
            # Reemplazar NaN por cadenas vacías para evitar errores en CH
            df_logs = df_logs.fillna('')
            
            # Insertar en ClickHouse
            ch_client.insert_df('bronze.logs_web', df_logs)
            print(f"✅ [logs_web] Ingestados {len(df_logs)} registros en Bronze.")
        else:
            print(f"❌ No se encuentra el fichero CSV: {path_logs_csv}")
    except Exception as e:
        print(f"❌ Error ingestando Logs: {e}")

    # ---------------------------------------------------------
    # 2. INGESTA USERS (Mongo) -> ClickHouse (bronze.users)
    #  "users.json" -> Desde colección "users" de MongoDB.
    # ---------------------------------------------------------
    try:
        # Recuperamos documentos de Mongo excluyendo el _id interno de mongo si no coincide,
        # pero el enunciado dice que el _id del json es la clave[cite: 86]. 
        # Mongo importa el campo "_id" del json como su id principal.
        cursor_users = mongo_db.users.find({})
        users_list = list(cursor_users)
        
        if users_list:
            # Preparamos los datos para ClickHouse
            # Convertimos todo a string para asegurar compatibilidad con Bronze
            data_to_insert = []
            for doc in users_list:
                row = [
                    str(doc.get('_id', '')),
                    str(doc.get('username', '')),
                    str(doc.get('email', '')),
                    str(doc.get('role', '')),
                    str(doc.get('country', '')),
                    str(doc.get('created_at', '')),
                    str(doc.get('is_premium', '')), # Convertimos bool a string 'True'/'False'
                    str(doc.get('risk_score', ''))
                ]
                data_to_insert.append(row)
            
            column_names = ['_id', 'username', 'email', 'role', 'country', 'created_at', 'is_premium', 'risk_score']
            
            ch_client.insert('bronze.users', data_to_insert, column_names=column_names)
            print(f"✅ [users] Ingestados {len(data_to_insert)} usuarios desde Mongo.")
        else:
            print("⚠️ La colección 'users' en Mongo está vacía.")
            
    except Exception as e:
        print(f"❌ Error ingestando Users: {e}")

    # ---------------------------------------------------------
    # 3. INGESTA IP_REPUTATION (Mongo) -> ClickHouse (bronze.ip_reputation)
    # "ip_reputation.json" -> Desde colección "ip_reputation".
    # ---------------------------------------------------------
    try:
        cursor_ips = mongo_db.ip_reputation.find({})
        ips_list = list(cursor_ips)
        
        if ips_list:
            data_to_insert = []
            for doc in ips_list:
                row = [
                    str(doc.get('ip', '')),
                    str(doc.get('source', '')),
                    str(doc.get('risk_level', '')),
                    str(doc.get('threat_type', '')),
                    str(doc.get('last_seen', ''))
                ]
                data_to_insert.append(row)

            column_names = ['ip', 'source', 'risk_level', 'threat_type', 'last_seen']
            
            ch_client.insert('bronze.ip_reputation', data_to_insert, column_names=column_names)
            print(f"✅ [ip_reputation] Ingestadas {len(data_to_insert)} IPs desde Mongo.")
        else:
            print("⚠️ La colección 'ip_reputation' en Mongo está vacía.")

    except Exception as e:
        print(f"❌ Error ingestando IP Reputation: {e}")

    # Cerrar conexiones
    mongo_client.close()
    # clickhouse_connect gestiona sus conexiones internamente, pero es bueno acabar limpio.
    print("-" * 30)
    print("🏁 Ingesta Bronze finalizada.")

if __name__ == "__main__":
    ingest_bronze()


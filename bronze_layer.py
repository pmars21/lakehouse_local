# bronze_layer.py

import os
import pandas as pd
from pymongo import MongoClient
import lakehouseConfig as lakehouseConfig
import mongo as mng

# Ruta portable al CSV (no absoluta)
ruta_data = os.path.join(os.getcwd(), "data")
path_logs_csv = os.path.join(ruta_data, 'logs_web.csv')


def ingest_bronze():
    ch_client = lakehouseConfig.get_client()
    mongo_client, mongo_db = mng.create_mongo_connection()
    
    print("🚀 Iniciando ingesta a Capa BRONZE...")

    # ---------------------------------------------------------
    # 1. INGESTA LOGS_WEB (CSV → ClickHouse)
    # ---------------------------------------------------------
    try:
        if os.path.exists(path_logs_csv):
            print(f"   Leyendo CSV: {path_logs_csv}...")

            df_logs = pd.read_csv(path_logs_csv, dtype=str).fillna('')

            ch_client.insert_df('bronze.logs_web', df_logs)
            print(f"✅ [logs_web] {len(df_logs)} registros insertados en Bronze.")

        else:
            print(f"❌ No se encuentra el fichero CSV: {path_logs_csv}")

    except Exception as e:
        print(f"❌ Error en logs_web: {e}")

    # ---------------------------------------------------------
    # 2. INGESTA USERS (Mongo → ClickHouse)
    # ---------------------------------------------------------
    try:
        users_list = list(mongo_db.users.find({}))

        if users_list:
            rows = []
            for doc in users_list:
                rows.append([
                    str(doc.get('_id', '')),
                    str(doc.get('username', '')),
                    str(doc.get('email', '')),
                    str(doc.get('role', '')),
                    str(doc.get('country', '')),
                    str(doc.get('created_at', '')),
                    str(doc.get('is_premium', '')),
                    str(doc.get('risk_score', ''))
                ])

            column_names = [
                '_id', 'username', 'email', 'role',
                'country', 'created_at', 'is_premium', 'risk_score'
            ]

            ch_client.insert('bronze.users', rows, column_names=column_names)
            print(f"✅ [users] {len(rows)} usuarios insertados en Bronze.")

        else:
            print("⚠️ La colección 'users' está vacía.")

    except Exception as e:
        print(f"❌ Error en users: {e}")

    # ---------------------------------------------------------
    # 3. INGESTA IP_REPUTATION (Mongo → ClickHouse)
    # ---------------------------------------------------------
    try:
        ip_list = list(mongo_db.ip_reputation.find({}))

        if ip_list:
            rows = []
            for doc in ip_list:
                rows.append([
                    str(doc.get('_id', '')),
                    str(doc.get('ip', '')),
                    str(doc.get('source', '')),
                    str(doc.get('risk_level', '')),
                    str(doc.get('threat_type', '')),
                    str(doc.get('last_seen', ''))
                ])

            column_names = [
                '_id', 'ip', 'source', 'risk_level',
                'threat_type', 'last_seen'
            ]

            ch_client.insert('bronze.ip_reputation', rows, column_names=column_names)
            print(f"✅ [ip_reputation] {len(rows)} IPs insertadas.")

        else:
            print("⚠️ La colección 'ip_reputation' está vacía.")

    except Exception as e:
        print(f"❌ Error en ip_reputation: {e}")

    # Cerrar conexiones
    mongo_client.close()
    print("--------------------------------------")
    print("🏁 Ingesta Bronze finalizada.")
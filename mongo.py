import json
from pymongo import MongoClient
import os
import config as conf


def create_mongo_connection():
    """
    Gestiona la conexión inicial y devuelve el cliente y la base de datos.
    """
    print(f"🔌 Intentando conectar a: {conf.db_uri} ...")
    client = MongoClient(conf.db_uri)
    db = client[conf.db_name]
    print(f"✅ Conexión establecida con la base de datos: {conf.db_name}")
    return client, db

def load_data_to_mongo():
    try:
        # 1. Conexión a MongoDB
        client, db = create_mongo_connection()

        # 2. Cargar users.json -> Colección 'users'
        # El enunciado pide explícitamente ingestarlo en la colección "users" 
        path_users = os.path.join(conf.ruta_data, 'users.json')
        if os.path.exists(path_users):
            with open(path_users, 'r', encoding='utf-8') as f:
                users_data = json.load(f)
                
            # Limpiar colección por si re-ejecutas el script (idempotencia)
            db.users.drop() 
            
            if isinstance(users_data, list) and len(users_data) > 0:
                db.users.insert_many(users_data)
                print(f"✅ Insertados {len(users_data)} documentos en colección 'users'.")
            else:
                print("⚠️ El fichero users.json está vacío o no es una lista.")
        else:
            print("❌ No se encuentra el fichero users.json")

        # 3. Cargar ip_reputation.json -> Colección 'ip_reputation'
        # El enunciado pide explícitamente ingestarlo en la colección "ip_reputation" [cite: 11]
        path_ip_reputation = os.path.join(conf.ruta_data, 'ip_reputation.json')
        if os.path.exists(path_ip_reputation):
            with open(path_ip_reputation, 'r', encoding='utf-8') as f:
                ip_data = json.load(f)
                
            db.ip_reputation.drop() # Limpiar anterior
            
            if isinstance(ip_data, list) and len(ip_data) > 0:
                db.ip_reputation.insert_many(ip_data)
                print(f"✅ Insertados {len(ip_data)} documentos en colección 'ip_reputation'.")
            else:
                print("⚠️ El fichero ip_reputation.json está vacío o no es una lista.")
        else:
            print("❌ No se encuentra el fichero ip_reputation.json")

    except Exception as e:
        print(f"❌ Error durante la carga a Mongo: {e}")
    finally:
        client.close()

#if __name__ == "__main__":
#    load_data_to_mongo()
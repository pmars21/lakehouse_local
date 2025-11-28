import json
from pymongo import MongoClient
import os
import config as conf


def create_mongo_connection():
    """
    Crea la conexión a MongoDB y devuelve el cliente y la base de datos.
    """
    print(f"🔌 Intentando conectar a MongoDB en: {conf.db_uri} ...")
    client = MongoClient(conf.db_uri)
    db = client[conf.db_name]
    print(f"✅ Conectado a la base de datos: {conf.db_name}")
    return client, db


def _load_json_file(path):
    """
    Carga un JSON desde disco y valida que sea una lista.
    """
    if not os.path.exists(path):
        print(f"❌ No se encuentra el fichero: {path}")
        return None

    with open(path, 'r', encoding='utf-8') as f:
        data = json.load(f)

    if not isinstance(data, list):
        print(f"⚠️ El JSON de {path} no contiene una lista. Formato no válido.")
        return None

    if len(data) == 0:
        print(f"⚠️ El JSON de {path} está vacío.")
        return None

    return data


def load_data_to_mongo():
    """
    Carga users.json e ip_reputation.json en sus colecciones correspondientes.
    El proceso es idempotente: limpia las colecciones antes de insertar.
    """
    try:
        client, db = create_mongo_connection()

        # Rutas a los ficheros
        path_users = os.path.join(conf.ruta_data, 'users.json')
        path_ips = os.path.join(conf.ruta_data, 'ip_reputation.json')

        # ---------------------------------------------------------
        # Insertar USERS
        # ---------------------------------------------------------
        users_data = _load_json_file(path_users)
        if users_data:
            db.users.drop()
            db.users.insert_many(users_data)
            print(f"✅ Insertados {len(users_data)} documentos en 'users'.")

        # ---------------------------------------------------------
        # Insertar IP_REPUTATION
        # ---------------------------------------------------------
        ip_data = _load_json_file(path_ips)
        if ip_data:
            db.ip_reputation.drop()
            db.ip_reputation.insert_many(ip_data)
            print(f"✅ Insertados {len(ip_data)} documentos en 'ip_reputation'.")

    except Exception as e:
        print(f"❌ Error durante la carga a Mongo: {e}")

    finally:
        try:
            client.close()
        except:
            pass
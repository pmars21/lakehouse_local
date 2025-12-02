import clickhouse_connect
import json
import config as conf



def get_client():
    # Leer el fichero de configuracion con los datos de conexion
    with open(conf.config_file, 'r', encoding='utf-8') as file:
        config = json.load(file)
    client = clickhouse_connect.get_client(
    host= config["host"],
    port = config["port"],
    username = config["username"],
    password = config["password"],
    secure = config["secure"]
)
    return client

def setup_lakehouse():
    client = get_client()
    print(" Conectado a ClickHouse.")

    # 1. CREAR LAS BASES DE DATOS (Capas del Lakehouse)
    # ---------------------------------------------------------
    databases = ['bronze', 'silver', 'gold']
    for db in databases:
        client.command(f"CREATE DATABASE IF NOT EXISTS {db}")
        print(f" Base de datos '{db}' lista.")

    # 2. CREAR TABLAS CAPA BRONZE (Estructura Raw)
    # ---------------------------------------------------------
    # Usamos motor MergeTree. En Bronze, usamos String/Nullable 
    # para asegurar que la ingesta no falle por formatos.
    
    # A. Tabla Logs Web (viene del CSV) 
    # Definimos columnas según anexo 
    client.command("""
    CREATE TABLE IF NOT EXISTS bronze.logs_web (
        event_id String,
        event_ts String,         -- Se convertirá a DateTime en Silver
        user_id String,
        ip_address String,
        http_method String,
        url_path String,
        status_code String,      -- String en Bronze para seguridad
        bytes_sent String,       
        response_time_ms String,
        user_agent String,
        is_suspicious String
    ) ENGINE = MergeTree()
    ORDER BY tuple()             -- En Bronze a veces no hay orden claro, o usas event_id
    """)
    print("Tabla 'bronze.logs_web' creada.")

    # B. Tabla Users (viene de Mongo -> users.json) [cite: 10]
    # Definimos columnas según anexo
    client.command("""
    CREATE TABLE IF NOT EXISTS bronze.users (
        _id String,
        username String,
        email String,
        role String,
        country String,
        created_at String,
        is_premium String,       -- Booleanos como String o Int en raw
        risk_score String
    ) ENGINE = MergeTree()
    ORDER BY _id
    """)
    print(" Tabla 'bronze.users' creada.")

    # C. Tabla IP Reputation (viene de Mongo -> ip_reputation.json) 
    # Definimos columnas según anexo 
    client.command("""
    CREATE TABLE IF NOT EXISTS bronze.ip_reputation (
        ip String,
        source String,
        risk_level String,
        threat_type String,
        last_seen String
    ) ENGINE = MergeTree()
    ORDER BY ip
    """)
    print("Tabla 'bronze.ip_reputation' creada.")
    
    print("-" * 30)
    print("Estructura Lakehouse inicializada correctamente.")

if __name__ == "__main__":
    try:
        setup_lakehouse()
    except Exception as e:
        print(f" Error conectando a ClickHouse: {e}")
        print("Asegúrate de que ClickHouse server está corriendo (docker o local).")
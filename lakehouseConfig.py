# lakehouseConfig.py
import clickhouse_connect
import json
import config as conf


def get_client():
    """
    Devuelve un cliente de conexión a ClickHouse usando config.json
    """
    with open(conf.config_file, 'r', encoding='utf-8') as file:
        config = json.load(file)

    client = clickhouse_connect.get_client(
        host=config["host"],
        port=config["port"],
        username=config["username"],
        password=config["password"],
        secure=config["secure"]
    )
    return client


def setup_lakehouse():
    client = get_client()
    print("🔌 Conectado a ClickHouse.")

    # ---------------------------------------------------------
    # 1. CREAR BASES DE DATOS
    # ---------------------------------------------------------
    for db in ['bronze', 'silver', 'gold']:
        client.command(f"CREATE DATABASE IF NOT EXISTS {db}")
        print(f"✅ Base de datos '{db}' creada.")

    # ---------------------------------------------------------
    # 2. TABLAS BRONZE (RAW)
    #    Todas las columnas como STRING para evitar errores
    # ---------------------------------------------------------

    # A. Logs Web
    client.command("""
    CREATE TABLE IF NOT EXISTS bronze.logs_web (
        event_id String,
        event_ts String,
        user_id String,
        ip_address String,
        http_method String,
        url_path String,
        status_code String,
        bytes_sent String,
        response_time_ms String,
        user_agent String,
        is_suspicious String
    )
    ENGINE = MergeTree()
    ORDER BY event_id
    """)
    print("✅ Tabla 'bronze.logs_web' lista.")

    # B. Users (desde MongoDB)
    client.command("""
    CREATE TABLE IF NOT EXISTS bronze.users (
        _id String,
        username String,
        email String,
        role String,
        country String,
        created_at String,
        is_premium String,
        risk_score String
    )
    ENGINE = MergeTree()
    ORDER BY _id
    """)
    print("✅ Tabla 'bronze.users' lista.")

    # C. IP Reputation (desde MongoDB)
    client.command("""
    CREATE TABLE IF NOT EXISTS bronze.ip_reputation (
        _id String,
        ip String,
        source String,
        risk_level String,
        threat_type String,
        last_seen String
    )
    ENGINE = MergeTree()
    ORDER BY _id
    """)
    print("✅ Tabla 'bronze.ip_reputation' lista.")

    print("-" * 40)
    print("🏛️  Lakehouse inicializado correctamente.")
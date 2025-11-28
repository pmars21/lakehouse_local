# gold_layer.py

import lakehouseConfig as lakehouseConfig


# ==========================================================
#   CREACIÓN DE TABLAS GOLD
# ==========================================================

def create_gold_tables(client):
    """
    Crea las tablas GOLD de seguridad, rendimiento y uso.
    """

    # ------------------------------
    # 1. Seguridad
    # ------------------------------
    client.command("""
    CREATE TABLE IF NOT EXISTS gold.security_daily (
        day Date,
        total_events UInt64,
        suspicious_events UInt64,
        high_risk_ip_events UInt64,
        brute_force_attempts UInt64,
        credential_stuffing_attempts UInt64,
        suspicious_login_attempts UInt64
    ) ENGINE = MergeTree()
    ORDER BY day
    """)
    print("✅ Tabla 'gold.security_daily' creada.")

    # ------------------------------
    # 2. Rendimiento
    # ------------------------------
    client.command("""
    CREATE TABLE IF NOT EXISTS gold.performance_daily (
        day Date,
        avg_latency_ms Float64,
        p95_latency_ms Float64,
        p99_latency_ms Float64,
        total_4xx UInt64,
        total_5xx UInt64
    ) ENGINE = MergeTree()
    ORDER BY day
    """)
    print("✅ Tabla 'gold.performance_daily' creada.")

    # ------------------------------
    # 3. Uso de usuario
    # ------------------------------
    client.command("""
    CREATE TABLE IF NOT EXISTS gold.usage_daily (
        day Date,
        role String,
        country String,
        is_premium UInt8,
        total_requests UInt64,
        unique_users UInt64,
        top_url String
    ) ENGINE = MergeTree()
    ORDER BY (day, role, country)
    """)
    print("✅ Tabla 'gold.usage_daily' creada.")


# ==========================================================
#   PROCESAMIENTO GOLD
# ==========================================================

def calculate_gold():
    """
    Calcula todas las métricas GOLD desde la tabla Silver.
    """
    client = lakehouseConfig.get_client()
    print("🔗 Conectado a ClickHouse para Capa GOLD.")

    # Crear tablas
    create_gold_tables(client)

    # Limpieza previa (idempotencia)
    client.command("TRUNCATE TABLE gold.security_daily")
    client.command("TRUNCATE TABLE gold.performance_daily")
    client.command("TRUNCATE TABLE gold.usage_daily")
    print("🧹 Tablas GOLD vaciadas.")

    # ======================================================
    # 1. Seguridad
    # ======================================================
    sql_security = """
    INSERT INTO gold.security_daily
    SELECT
        toDate(event_ts) AS day,
        count() AS total_events,
        sum(is_suspicious_calc) AS suspicious_events,
        countIf(ip_risk_level IN ('high', 'critical')) AS high_risk_ip_events,
        countIf(ip_threat_type = 'brute_force') AS brute_force_attempts,
        countIf(ip_threat_type = 'credential_stuffing') AS credential_stuffing_attempts,
        countIf(ip_threat_type = 'suspicious_login') AS suspicious_login_attempts
    FROM silver.logs_enriched
    GROUP BY day
    ORDER BY day
    """
    client.command(sql_security)
    print("🔐 Seguridad GOLD cargada.")

    # ======================================================
    # 2. Rendimiento
    # ======================================================
    sql_performance = """
    INSERT INTO gold.performance_daily
    SELECT
        toDate(event_ts) AS day,
        avg(response_time_ms) AS avg_latency_ms,
        quantile(0.95)(response_time_ms) AS p95_latency_ms,
        quantile(0.99)(response_time_ms) AS p99_latency_ms,
        countIf(status_class = '4xx') AS total_4xx,
        countIf(status_class = '5xx') AS total_5xx
    FROM silver.logs_enriched
    GROUP BY day
    ORDER BY day
    """
    client.command(sql_performance)
    print("🚀 Rendimiento GOLD cargado.")


    # ======================================================
    # 3. Uso / comportamiento (versión compatible con CH Cloud)
    # ======================================================
    sql_usage = """
    WITH ranked_urls AS 
    (
        SELECT
            toDate(event_ts) AS day,
            role,
            url_path,
            count() AS cnt,
            row_number() OVER (
                PARTITION BY toDate(event_ts), role
                ORDER BY count() DESC
            ) AS rn
        FROM silver.logs_enriched
        GROUP BY day, role, url_path
    )
    INSERT INTO gold.usage_daily
    SELECT
        toDate(s.event_ts) AS day,
        s.role,
        s.country,
        s.is_premium,
        count() AS total_requests,
        countDistinct(s.user_id) AS unique_users,
        argMax(r.url_path, r.cnt) AS top_url
    FROM silver.logs_enriched AS s
    LEFT JOIN ranked_urls AS r
        ON r.day = toDate(s.event_ts)
       AND r.role = s.role
       AND r.rn = 1
    GROUP BY day, role, country, is_premium
    ORDER BY day
    """
    client.command(sql_usage)
    print("👤 Uso GOLD cargado.")

    print("🏁 Cálculo GOLD completado.")
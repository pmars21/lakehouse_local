# silver_layer.py

import lakehouseConfig as lakehouseConfig


def create_silver_table(client):
    """
    Crea la tabla principal de la capa SILVER si no existe.
    Tabla tipada y enriquecida con información de usuario e IP.
    """
    client.command("""
    CREATE TABLE IF NOT EXISTS silver.logs_enriched (
        event_id String,
        event_ts DateTime,
        user_id String,
        username String,
        email String,
        role String,
        country String,
        created_at DateTime,
        is_premium UInt8,
        risk_score Float32,
        ip_address String,
        ip_risk_level String,
        ip_threat_type String,
        http_method String,
        url_path String,
        status_code Int32,
        status_class String,
        bytes_sent UInt64,
        response_time_ms UInt32,
        user_agent String,
        is_suspicious_raw UInt8,
        is_suspicious_calc UInt8
    )
    ENGINE = MergeTree()
    ORDER BY (event_ts, event_id)
    """)
    print("✅ Tabla 'silver.logs_enriched' lista.")


def process_silver():
    """
    Procesa los datos desde Bronze y genera la tabla Silver:
    - Convierte tipos
    - Hace JOIN con users e ip_reputation
    - Calcula flags de riesgo y clases de status
    """
    client = lakehouseConfig.get_client()
    print("🔗 Conectado a ClickHouse para procesar SILVER.")

    # 1. Crear tabla Silver si no existe
    create_silver_table(client)

    # 2. Limpiar datos anteriores para que el proceso sea idempotente
    client.command("TRUNCATE TABLE silver.logs_enriched")
    print("🧹 Tabla 'silver.logs_enriched' vaciada antes de recargar datos.")

    # 3. Insertar datos transformados desde Bronze
    #    Usamos una única sentencia INSERT ... SELECT
    insert_sql = """
    INSERT INTO silver.logs_enriched
    SELECT
        l.event_id                                           AS event_id,
        -- Convertimos el timestamp a DateTime; si falla, se queda en 1970-01-01
        ifNull(parseDateTimeBestEffortOrNull(l.event_ts), toDateTime('1970-01-01 00:00:00')) AS event_ts,

        l.user_id                                            AS user_id,
        u.username                                           AS username,
        u.email                                              AS email,
        u.role                                               AS role,
        u.country                                            AS country,
        ifNull(parseDateTimeBestEffortOrNull(u.created_at), toDateTime('1970-01-01 00:00:00')) AS created_at,

        -- is_premium (string -> boolean -> UInt8)
        if(
            lower(trim(u.is_premium)) IN ('1','true','t','yes'),
            1, 0
        )                                                     AS is_premium,

        -- risk_score a Float32
        ifNull(toFloat32OrNull(u.risk_score), 0.0)            AS risk_score,

        l.ip_address                                          AS ip_address,
        ip.risk_level                                         AS ip_risk_level,
        ip.threat_type                                        AS ip_threat_type,

        l.http_method                                         AS http_method,
        l.url_path                                            AS url_path,

        -- status_code a Int32
        ifNull(toInt32OrNull(l.status_code), 0)               AS status_code,

        -- Clasificación de status_code
        multiIf(
            status_code BETWEEN 200 AND 299, '2xx',
            status_code BETWEEN 300 AND 399, '3xx',
            status_code BETWEEN 400 AND 499, '4xx',
            status_code BETWEEN 500 AND 599, '5xx',
            'other'
        )                                                     AS status_class,

        -- bytes_sent y response_time tipados
        ifNull(toUInt64OrNull(l.bytes_sent), 0)               AS bytes_sent,
        ifNull(toUInt32OrNull(l.response_time_ms), 0)         AS response_time_ms,

        l.user_agent                                          AS user_agent,

        -- is_suspicious original del log, como flag binario
        if(
            lower(trim(l.is_suspicious)) IN ('1','true','t','yes'),
            1, 0
        )                                                     AS is_suspicious_raw,

        -- is_suspicious calculado combinando:
        --  * IP de riesgo (high/critical)
        --  * threat_type distinto de benign/interno
        --  * ciertos códigos HTTP (errores auth/servidor)
        if(
            (
                ip.risk_level IN ('high', 'critical')
                OR (ip.threat_type NOT IN ('', 'benign') AND ip.threat_type IS NOT NULL)
                OR status_code IN (401, 403, 429, 500, 503)
            ),
            1, 0
        )                                                     AS is_suspicious_calc

    FROM bronze.logs_web AS l
    LEFT JOIN bronze.users AS u
        ON l.user_id = u._id
    LEFT JOIN bronze.ip_reputation AS ip
        ON l.ip_address = ip.ip
    """

    try:
        client.command(insert_sql)
        print("✅ Datos cargados en 'silver.logs_enriched'.")
    except Exception as e:
        print(f"❌ Error procesando SILVER: {e}")
        raise

    print("🏁 Procesamiento de Capa SILVER completado.")
import clickhouse_connect
import time
import lakehouseConfig as conf


def process_silver():
    client = conf.get_client()
    print("🚀 Iniciando procesamiento Capa SILVER...")
    start_time = time.time()

    # 1. DEFINICIÓN DE TABLA SILVER (DDL)
    # ---------------------------------------------------------
    # Creamos una tabla "aplanada" que junta info de logs, usuarios e IPs.
    # Usamos motor MergeTree ordenado por tiempo.
    
    ddl_silver = """
    CREATE TABLE IF NOT EXISTS silver.enriched_events (
        -- Datos del Log Original
        event_id String,
        event_ts DateTime,
        user_id String,
        ip_address String,
        http_method String,
        url_path String,
        status_code Int32,
        bytes_sent Int32,
        response_time_ms Int32,
        user_agent String,
        is_suspicious UInt8,
        
        -- Datos Enriquecidos del Usuario (JOIN con users)
        user_name String,
        user_email String,
        user_role String,
        user_country String,
        user_is_premium Bool,
        
        -- Datos Enriquecidos de IP (JOIN con ip_reputation)
        ip_risk_level String,
        ip_threat_type String,
        ip_source String
        
    ) ENGINE = MergeTree()
    ORDER BY event_ts
    """
    client.command(ddl_silver)
    print("✅ Tabla 'silver.enriched_events' verificada.")

    # 2. LIMPIEZA PREVIA (Idempotencia)
    # ---------------------------------------------------------
    # Borramos datos antiguos para recargar (en un entorno real usaríamos particiones)
    client.command("TRUNCATE TABLE silver.enriched_events")
    print("🧹 Tabla Silver limpiada para nueva carga.")

    # 3. TRANSFORMACIÓN Y CARGA (ETL via SQL)
    # ---------------------------------------------------------
    # Hacemos LEFT JOIN porque:
    # - Puede haber logs de usuarios no registrados (user_id vacío) -> LEFT JOIN users
    # - Puede haber IPs que no estén en nuestra lista de reputación -> LEFT JOIN ip_reputation
    
    sql_insert = """
    INSERT INTO silver.enriched_events
    SELECT
        -- Campos de Logs (bronze.logs_web)
        L.event_id,
        parseDateTimeBestEffort(L.event_ts) AS event_ts, -- Conversión a DateTime
        L.user_id,
        L.ip_address,
        L.http_method,
        L.url_path,
        L.status_code,
        ifNull(L.bytes_sent, 0),        -- Limpieza de Nulos
        ifNull(L.response_time_ms, 0),  -- Limpieza de Nulos
        L.user_agent,
        L.is_suspicious,

        -- Campos de Users (bronze.users)
        -- Si no cruza (usuario anónimo), ponemos valores por defecto
        if(U.username = '', 'Anonymous', U.username) as user_name,
        U.email,
        if(U.role = '', 'guest', U.role) as user_role, -- [cite: 94]
        if(U.country = '', 'XX', U.country) as user_country,
        ifNull(U.is_premium, 0) as user_is_premium,

        -- Campos de IP Reputation (bronze.ip_reputation)
        -- Si no cruza, asumimos riesgo bajo/desconocido
        if(I.risk_level = '', 'unknown', I.risk_level) as ip_risk_level, -- [cite: 133]
        if(I.threat_type = '', 'benign', I.threat_type) as ip_threat_type, -- 
        I.source

    FROM bronze.logs_web AS L
    LEFT JOIN bronze.users AS U 
        ON L.user_id = U._id  -- Cruce por ID de usuario [cite: 88]
    LEFT JOIN bronze.ip_reputation AS I 
        ON L.ip_address = I.ip -- Cruce por IP [cite: 121]

        WHERE L.user_id IS NOT NULL 
        AND L.user_id != ''
    """
    
    client.command(sql_insert)
    
    # 4. VERIFICACIÓN
    # ---------------------------------------------------------
    count = client.command("SELECT count() FROM silver.enriched_events")
    duration = time.time() - start_time
    
    print(f"✅ Procesamiento Silver completado.")
    print(f"📊 Registros generados: {count}")
    print(f"⏱️  Tiempo de ejecución: {duration:.2f} segundos")
    print("-" * 30)

if __name__ == "__main__":
    process_silver()
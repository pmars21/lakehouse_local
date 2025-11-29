"""
================================================================================
CAPA GOLD - MÉTRICAS Y AGREGACIONES DE NEGOCIO
================================================================================
Este script implementa la capa Gold del Lakehouse:
- Lee datos de la capa Silver
- Calcula métricas agregadas
- Genera KPIs de negocio
- Crea tablas optimizadas para consumo analítico

La capa Gold contiene datos listos para dashboards, reportes y análisis,
con agregaciones pre-calculadas para rendimiento óptimo.

Tablas Gold creadas:
1. gold_daily_traffic_metrics - Métricas diarias de tráfico
2. gold_user_activity_metrics - Métricas por usuario
3. gold_ip_threat_analysis - Análisis de amenazas por IP
4. gold_security_summary - Resumen de seguridad
5. gold_hourly_patterns - Patrones por hora/día

Autor: [Tu nombre]
Fecha: 2025
================================================================================
"""

import json
import clickhouse_connect
import lakehouseConfig as conf


# Conectar
client = conf.get_client()
    
    
     # --- CORRECCIÓN 1: CREAR LA BASE DE DATOS ---
print("🛠️ Asegurando que existe la base de datos 'lakehouse'...")
client.command("CREATE DATABASE IF NOT EXISTS lakehouse")
        # ---------------------------------------------

        # Crear tablas Gold
print("\n" + "-"*60)
print("📋 CREANDO TABLAS GOLD")
    # ... (resto del código igual)

# =============================================================================
# CREACIÓN DE TABLAS GOLD
# =============================================================================

def create_gold_daily_traffic_table(client):
    """
    Crea tabla de métricas diarias de tráfico.
    
    Métricas incluidas:
    - Total de requests
    - Usuarios únicos
    - IPs únicas
    - Bytes totales
    - Tiempos de respuesta (promedio y percentil 95)
    - Tasa de errores
    - Tasa de eventos sospechosos
    """
    print("\n📋 Creando tabla gold_daily_traffic_metrics...")
    
    client.command("DROP TABLE IF EXISTS lakehouse.gold_daily_traffic_metrics")
    
    client.command("""
        CREATE TABLE lakehouse.gold_daily_traffic_metrics (
            event_date Date COMMENT 'Fecha',
            
            -- Volumen
            total_requests UInt64 COMMENT 'Total de peticiones',
            unique_users UInt64 COMMENT 'Usuarios únicos',
            unique_ips UInt64 COMMENT 'IPs únicas',
            total_bytes_sent UInt64 COMMENT 'Bytes totales enviados',
            
            -- Rendimiento
            avg_response_time_ms Float64 COMMENT 'Tiempo respuesta promedio',
            p95_response_time_ms Float64 COMMENT 'Percentil 95 tiempo respuesta',
            max_response_time_ms UInt32 COMMENT 'Tiempo respuesta máximo',
            
            -- Errores
            error_count UInt64 COMMENT 'Total de errores (4xx + 5xx)',
            client_error_count UInt64 COMMENT 'Errores cliente (4xx)',
            server_error_count UInt64 COMMENT 'Errores servidor (5xx)',
            error_rate Float64 COMMENT 'Tasa de errores',
            
            -- Seguridad
            suspicious_count UInt64 COMMENT 'Eventos sospechosos',
            suspicious_rate Float64 COMMENT 'Tasa de sospechosos',
            bot_requests UInt64 COMMENT 'Peticiones de bots',
            
            -- Metadatos
            _calculated_at DateTime DEFAULT now()
        )
        ENGINE = MergeTree()
        ORDER BY (event_date)
        COMMENT 'Capa Gold: Métricas diarias de tráfico web'
    """)
    print("   ✅ Tabla creada")


def create_gold_user_activity_table(client):
    """
    Crea tabla de métricas de actividad por usuario.
    
    Métricas incluidas:
    - Actividad total y temporal
    - Intentos de login y fallos
    - Consumo de recursos
    - Score de riesgo combinado
    """
    print("\n📋 Creando tabla gold_user_activity_metrics...")
    
    client.command("DROP TABLE IF EXISTS lakehouse.gold_user_activity_metrics")
    
    client.command("""
        CREATE TABLE lakehouse.gold_user_activity_metrics (
            -- Identificación
            user_id String COMMENT 'ID del usuario',
            username String COMMENT 'Nombre de usuario',
            user_role String COMMENT 'Rol',
            user_country String COMMENT 'País',
            is_premium UInt8 COMMENT 'Usuario premium',
            
            -- Actividad
            total_requests UInt64 COMMENT 'Total peticiones',
            first_activity DateTime COMMENT 'Primera actividad',
            last_activity DateTime COMMENT 'Última actividad',
            distinct_days_active UInt32 COMMENT 'Días distintos con actividad',
            
            -- Autenticación
            login_attempts UInt64 COMMENT 'Intentos de login',
            failed_logins UInt64 COMMENT 'Logins fallidos',
            login_success_rate Float64 COMMENT 'Tasa de éxito en login',
            
            -- Consumo
            avg_response_time_ms Float64 COMMENT 'Tiempo respuesta promedio',
            total_bytes_consumed UInt64 COMMENT 'Bytes totales consumidos',
            
            -- Comportamiento
            distinct_ips_used UInt32 COMMENT 'IPs distintas usadas',
            distinct_urls_accessed UInt32 COMMENT 'URLs distintas accedidas',
            admin_access_count UInt64 COMMENT 'Accesos a admin',
            
            -- Riesgo
            error_count UInt64 COMMENT 'Errores generados',
            suspicious_events UInt64 COMMENT 'Eventos sospechosos',
            original_risk_score Float64 COMMENT 'Score de riesgo original',
            combined_risk_score Float64 COMMENT 'Score de riesgo calculado',
            
            -- Metadatos
            _calculated_at DateTime DEFAULT now()
        )
        ENGINE = MergeTree()
        ORDER BY (combined_risk_score DESC, user_id)
        COMMENT 'Capa Gold: Métricas de actividad por usuario'
    """)
    print("   ✅ Tabla creada")


def create_gold_ip_threat_table(client):
    """
    Crea tabla de análisis de amenazas por IP.
    
    Métricas incluidas:
    - Información de reputación
    - Actividad detectada
    - Usuarios afectados
    - Score de amenaza calculado
    """
    print("\n📋 Creando tabla gold_ip_threat_analysis...")
    
    client.command("DROP TABLE IF EXISTS lakehouse.gold_ip_threat_analysis")
    
    client.command("""
        CREATE TABLE lakehouse.gold_ip_threat_analysis (
            -- Identificación
            ip_address String COMMENT 'Dirección IP',
            ip_risk_level String COMMENT 'Nivel de riesgo de la IP',
            ip_threat_type String COMMENT 'Tipo de amenaza',
            ip_source String COMMENT 'Fuente de inteligencia',
            
            -- Actividad
            total_requests UInt64 COMMENT 'Total peticiones desde esta IP',
            first_seen DateTime COMMENT 'Primera vez vista',
            last_seen DateTime COMMENT 'Última vez vista',
            days_active UInt32 COMMENT 'Días con actividad',
            
            -- Impacto
            distinct_users_affected UInt32 COMMENT 'Usuarios distintos afectados',
            distinct_urls_accessed UInt32 COMMENT 'URLs distintas accedidas',
            
            -- Comportamiento sospechoso
            login_attempts UInt64 COMMENT 'Intentos de login',
            failed_logins UInt64 COMMENT 'Logins fallidos',
            admin_access_attempts UInt64 COMMENT 'Intentos de acceso admin',
            suspicious_events UInt64 COMMENT 'Eventos marcados sospechosos',
            error_events UInt64 COMMENT 'Eventos con error',
            
            -- Puntuación de amenaza
            threat_score Float64 COMMENT 'Score de amenaza (0-5)',
            
            -- Metadatos
            _calculated_at DateTime DEFAULT now()
        )
        ENGINE = MergeTree()
        ORDER BY (threat_score DESC, ip_address)
        COMMENT 'Capa Gold: Análisis de amenazas por IP'
    """)
    print("   ✅ Tabla creada")


def create_gold_security_summary_table(client):
    """
    Crea tabla de resumen de seguridad diario.
    
    Métricas incluidas:
    - Total de eventos y eventos de riesgo
    - Tipos de amenazas detectadas
    - Usuarios y cuentas afectadas
    - Score promedio de amenaza
    """
    print("\n📋 Creando tabla gold_security_summary...")
    
    client.command("DROP TABLE IF EXISTS lakehouse.gold_security_summary")
    
    client.command("""
        CREATE TABLE lakehouse.gold_security_summary (
            summary_date Date COMMENT 'Fecha del resumen',
            
            -- Volumen
            total_events UInt64 COMMENT 'Total de eventos',
            high_risk_events UInt64 COMMENT 'Eventos de alto riesgo',
            
            -- IPs
            total_unique_ips UInt32 COMMENT 'IPs únicas totales',
            critical_ips_active UInt32 COMMENT 'IPs críticas activas',
            high_risk_ips_active UInt32 COMMENT 'IPs de alto riesgo activas',
            
            -- Tipos de amenaza
            brute_force_attempts UInt64 COMMENT 'Intentos brute force',
            credential_stuffing_attempts UInt64 COMMENT 'Intentos credential stuffing',
            suspicious_logins UInt64 COMMENT 'Logins sospechosos',
            
            -- Usuarios afectados
            total_users_active UInt32 COMMENT 'Usuarios activos totales',
            premium_users_affected UInt32 COMMENT 'Usuarios premium afectados',
            admin_accounts_targeted UInt32 COMMENT 'Cuentas admin objetivo',
            
            -- Métricas agregadas
            avg_threat_score Float64 COMMENT 'Score de amenaza promedio',
            max_threat_score Float64 COMMENT 'Score de amenaza máximo',
            top_threat_type String COMMENT 'Tipo de amenaza principal',
            
            -- Metadatos
            _calculated_at DateTime DEFAULT now()
        )
        ENGINE = MergeTree()
        ORDER BY (summary_date)
        COMMENT 'Capa Gold: Resumen de seguridad diario'
    """)
    print("   ✅ Tabla creada")


def create_gold_hourly_patterns_table(client):
    """
    Crea tabla de patrones por hora y día de la semana.
    
    Útil para:
    - Identificar patrones de uso normal
    - Detectar anomalías temporales
    - Planificación de capacidad
    """
    print("\n📋 Creando tabla gold_hourly_patterns...")
    
    client.command("DROP TABLE IF EXISTS lakehouse.gold_hourly_patterns")
    
    client.command("""
        CREATE TABLE lakehouse.gold_hourly_patterns (
            event_hour UInt8 COMMENT 'Hora del día (0-23)',
            day_of_week UInt8 COMMENT 'Día de la semana (1=Lun, 7=Dom)',
            
            -- Volumen
            total_requests UInt64 COMMENT 'Total de peticiones',
            unique_users UInt32 COMMENT 'Usuarios únicos',
            unique_ips UInt32 COMMENT 'IPs únicas',
            
            -- Rendimiento
            avg_response_time_ms Float64 COMMENT 'Tiempo respuesta promedio',
            
            -- Tasas
            error_rate Float64 COMMENT 'Tasa de errores',
            suspicious_rate Float64 COMMENT 'Tasa de sospechosos',
            bot_rate Float64 COMMENT 'Tasa de bots',
            
            -- Categorías más comunes
            top_url_category String COMMENT 'Categoría URL más común',
            
            -- Metadatos
            _calculated_at DateTime DEFAULT now()
        )
        ENGINE = MergeTree()
        ORDER BY (day_of_week, event_hour)
        COMMENT 'Capa Gold: Patrones de tráfico por hora y día'
    """)
    print("   ✅ Tabla creada")

# =============================================================================
# CÁLCULO DE MÉTRICAS
# =============================================================================

def calculate_daily_traffic_metrics(client):
    """Calcula métricas diarias de tráfico."""
    print("\n📈 Calculando gold_daily_traffic_metrics...")
    
    client.command("TRUNCATE TABLE lakehouse.gold_daily_traffic_metrics")
    
    client.command("""
        INSERT INTO lakehouse.gold_daily_traffic_metrics
        SELECT
            event_date,
            
            -- Volumen
            count() AS total_requests,
            uniqExact(user_id) AS unique_users,
            uniqExact(ip_address) AS unique_ips,
            sum(bytes_sent) AS total_bytes_sent,
            
            -- Rendimiento
            avg(response_time_ms) AS avg_response_time_ms,
            quantile(0.95)(response_time_ms) AS p95_response_time_ms,
            max(response_time_ms) AS max_response_time_ms,
            
            -- Errores
            countIf(is_error = 1) AS error_count,
            countIf(status_code >= 400 AND status_code < 500) AS client_error_count,
            countIf(status_code >= 500) AS server_error_count,
            countIf(is_error = 1) / count() AS error_rate,
            
            -- Seguridad
            countIf(is_suspicious = 1) AS suspicious_count,
            countIf(is_suspicious = 1) / count() AS suspicious_rate,
            countIf(is_bot = 1) AS bot_requests
            
        FROM lakehouse.silver_logs_enriched
        GROUP BY event_date
        ORDER BY event_date
    """)
    
    count = client.command("SELECT count() FROM lakehouse.gold_daily_traffic_metrics")
    print(f"   ✅ {count} registros calculados")


def calculate_user_activity_metrics(client):
    """Calcula métricas de actividad por usuario."""
    print("\n📈 Calculando gold_user_activity_metrics...")
    
    client.command("TRUNCATE TABLE lakehouse.gold_user_activity_metrics")
    
    client.command("""
        INSERT INTO lakehouse.gold_user_activity_metrics
        SELECT
            -- Identificación
            user_id,
            any(username) AS username,
            any(user_role) AS user_role,
            any(user_country) AS user_country,
            any(user_is_premium) AS is_premium,
            
            -- Actividad
            count() AS total_requests,
            min(event_ts) AS first_activity,
            max(event_ts) AS last_activity,
            uniqExact(event_date) AS distinct_days_active,
            
            -- Autenticación
            countIf(url_category = 'authentication') AS login_attempts,
            countIf(url_category = 'authentication' AND status_code IN (401, 403)) AS failed_logins,
            -- Tasa de éxito en login
            if(
                countIf(url_category = 'authentication') > 0,
                1 - (countIf(url_category = 'authentication' AND status_code IN (401, 403)) 
                     / countIf(url_category = 'authentication')),
                1.0
            ) AS login_success_rate,
            
            -- Consumo
            avg(response_time_ms) AS avg_response_time_ms,
            sum(bytes_sent) AS total_bytes_consumed,
            
            -- Comportamiento
            uniqExact(ip_address) AS distinct_ips_used,
            uniqExact(url_path) AS distinct_urls_accessed,
            countIf(url_category = 'admin') AS admin_access_count,
            
            -- Riesgo
            countIf(is_error = 1) AS error_count,
            countIf(is_suspicious = 1) AS suspicious_events,
            any(user_risk_score) AS original_risk_score,
            
            -- Score combinado de riesgo
            -- Factores: riesgo original, tasa de errores, eventos sospechosos, IPs múltiples
            (any(user_risk_score) * 0.3) + 
            (countIf(is_suspicious = 1) / greatest(count(), 1) * 0.25) +
            (countIf(url_category = 'authentication' AND status_code IN (401, 403)) 
                / greatest(count(), 1) * 0.25) +
            (least(uniqExact(ip_address), 10) / 10.0 * 0.2) AS combined_risk_score
            
        FROM lakehouse.silver_logs_enriched
        WHERE user_id != '' AND username != 'anonymous'
        GROUP BY user_id
        ORDER BY combined_risk_score DESC
    """)
    
    count = client.command("SELECT count() FROM lakehouse.gold_user_activity_metrics")
    print(f"   ✅ {count} registros calculados")
    
    # Mostrar top usuarios de riesgo
    result = client.query("""
        SELECT username, user_role, round(combined_risk_score, 3) as risk
        FROM lakehouse.gold_user_activity_metrics
        ORDER BY combined_risk_score DESC
        LIMIT 3
    """)
    print("   🔴 Top 3 usuarios por riesgo:")
    for row in result.result_rows:
        print(f"      - {row[0]} ({row[1]}): {row[2]}")


def calculate_ip_threat_analysis(client):
    """Calcula análisis de amenazas por IP."""
    print("\n📈 Calculando gold_ip_threat_analysis...")
    
    client.command("TRUNCATE TABLE lakehouse.gold_ip_threat_analysis")
    
    client.command("""
        INSERT INTO lakehouse.gold_ip_threat_analysis
        SELECT
            -- Identificación
            ip_address,
            any(ip_risk_level) AS ip_risk_level,
            any(ip_threat_type) AS ip_threat_type,
            any(ip_source) AS ip_source,
            
            -- Actividad
            count() AS total_requests,
            min(event_ts) AS first_seen,
            max(event_ts) AS last_seen,
            uniqExact(event_date) AS days_active,
            
            -- Impacto
            uniqExact(user_id) AS distinct_users_affected,
            uniqExact(url_path) AS distinct_urls_accessed,
            
            -- Comportamiento sospechoso
            countIf(url_category = 'authentication') AS login_attempts,
            countIf(url_category = 'authentication' AND status_code IN (401, 403)) AS failed_logins,
            countIf(url_category = 'admin') AS admin_access_attempts,
            countIf(is_suspicious = 1) AS suspicious_events,
            countIf(is_error = 1) AS error_events,
            
            -- Threat Score calculado
            -- Componentes:
            -- 1. Nivel de riesgo base de la IP (30%)
            -- 2. Tasa de eventos sospechosos (25%)
            -- 3. Tasa de intentos de admin (25%)
            -- 4. Tasa de fallos de autenticación (20%)
            (
                CASE any(ip_risk_level)
                    WHEN 'critical' THEN 4
                    WHEN 'high' THEN 3
                    WHEN 'medium' THEN 2
                    WHEN 'low' THEN 1
                    ELSE 0
                END * 0.30
            ) +
            (countIf(is_suspicious = 1) / greatest(count(), 1) * 1.25) +
            (countIf(url_category = 'admin') / greatest(count(), 1) * 1.25) +
            (countIf(status_code IN (401, 403)) / greatest(count(), 1) * 1.0) AS threat_score
            
        FROM lakehouse.silver_logs_enriched
        GROUP BY ip_address
        ORDER BY threat_score DESC
    """)
    
    count = client.command("SELECT count() FROM lakehouse.gold_ip_threat_analysis")
    print(f"   ✅ {count} registros calculados")
    
    # Mostrar IPs más amenazantes
    result = client.query("""
        SELECT 
            ip_address, 
            ip_risk_level, 
            ip_threat_type,
            round(threat_score, 3) as score
        FROM lakehouse.gold_ip_threat_analysis
        WHERE ip_risk_level != 'unknown'
        ORDER BY threat_score DESC
        LIMIT 5
    """)
    print("   🔴 Top 5 IPs por amenaza:")
    for row in result.result_rows:
        print(f"      - {row[0]} [{row[1]}] {row[2]}: {row[3]}")


def calculate_security_summary(client):
    """Calcula resumen de seguridad diario."""
    print("\n📈 Calculando gold_security_summary...")
    
    client.command("TRUNCATE TABLE lakehouse.gold_security_summary")
    
    client.command("""
        INSERT INTO lakehouse.gold_security_summary
        SELECT
            event_date AS summary_date,
            
            -- Volumen
            count() AS total_events,
            countIf(ip_risk_level IN ('high', 'critical') OR is_suspicious = 1) AS high_risk_events,
            
            -- IPs
            uniqExact(ip_address) AS total_unique_ips,
            uniqExactIf(ip_address, ip_risk_level = 'critical') AS critical_ips_active,
            uniqExactIf(ip_address, ip_risk_level = 'high') AS high_risk_ips_active,
            
            -- Tipos de amenaza
            countIf(ip_threat_type = 'brute_force') AS brute_force_attempts,
            countIf(ip_threat_type = 'credential_stuffing') AS credential_stuffing_attempts,
            countIf(ip_threat_type = 'suspicious_login') AS suspicious_logins,
            
            -- Usuarios afectados
            uniqExact(user_id) AS total_users_active,
            uniqExactIf(user_id, user_is_premium = 1 AND 
                (is_suspicious = 1 OR ip_risk_level IN ('high', 'critical'))) AS premium_users_affected,
            uniqExactIf(user_id, user_role = 'admin' AND 
                (is_suspicious = 1 OR ip_risk_level IN ('high', 'critical'))) AS admin_accounts_targeted,
            
            -- Métricas agregadas
            avgIf(
                CASE ip_risk_level
                    WHEN 'critical' THEN 4
                    WHEN 'high' THEN 3
                    WHEN 'medium' THEN 2
                    WHEN 'low' THEN 1
                    ELSE 0
                END,
                ip_risk_level != 'unknown'
            ) AS avg_threat_score,
            maxIf(
                CASE ip_risk_level
                    WHEN 'critical' THEN 4
                    WHEN 'high' THEN 3
                    WHEN 'medium' THEN 2
                    WHEN 'low' THEN 1
                    ELSE 0
                END,
                ip_risk_level != 'unknown'
            ) AS max_threat_score,
            -- Tipo de amenaza más común (excluyendo benignas)
            anyIf(ip_threat_type, 
                ip_threat_type NOT IN ('benign', 'internal_traffic', 'unknown')) AS top_threat_type
            
        FROM lakehouse.silver_logs_enriched
        GROUP BY event_date
        ORDER BY summary_date
    """)
    
    count = client.command("SELECT count() FROM lakehouse.gold_security_summary")
    print(f"   ✅ {count} registros calculados")


def calculate_hourly_patterns(client):
    """Calcula patrones de tráfico por hora y día."""
    print("\n📈 Calculando gold_hourly_patterns...")
    
    client.command("TRUNCATE TABLE lakehouse.gold_hourly_patterns")
    
    client.command("""
        INSERT INTO lakehouse.gold_hourly_patterns
        SELECT
            event_hour,
            event_day_of_week AS day_of_week,
            
            -- Volumen
            count() AS total_requests,
            uniqExact(user_id) AS unique_users,
            uniqExact(ip_address) AS unique_ips,
            
            -- Rendimiento
            avg(response_time_ms) AS avg_response_time_ms,
            
            -- Tasas
            countIf(is_error = 1) / count() AS error_rate,
            countIf(is_suspicious = 1) / count() AS suspicious_rate,
            countIf(is_bot = 1) / count() AS bot_rate,
            
            -- Categoría más común
            topK(1)(url_category)[1] AS top_url_category
            
        FROM lakehouse.silver_logs_enriched
        GROUP BY event_hour, event_day_of_week
        ORDER BY day_of_week, event_hour
    """)
    
    count = client.command("SELECT count() FROM lakehouse.gold_hourly_patterns")
    print(f"   ✅ {count} registros calculados")

# =============================================================================
# VERIFICACIÓN Y CONSULTAS DE EJEMPLO
# =============================================================================

def verify_gold_layer(client):
    """Verifica la capa Gold y muestra estadísticas."""
    print("\n" + "="*60)
    print("🔍 VERIFICACIÓN CAPA GOLD")
    print("="*60)
    
    tables = [
        'gold_daily_traffic_metrics',
        'gold_user_activity_metrics',
        'gold_ip_threat_analysis',
        'gold_security_summary',
        'gold_hourly_patterns'
    ]
    
    print("\n📊 Conteo de registros:")
    for table in tables:
        count = client.command(f"SELECT count() FROM lakehouse.{table}")
        print(f"   - {table}: {count}")


def show_gold_insights(client):
    """Muestra insights de las tablas Gold."""
    print("\n" + "="*60)
    print("💡 INSIGHTS DE LA CAPA GOLD")
    print("="*60)
    
    # Insight 1: Resumen de tráfico
    print("\n📊 Resumen de Tráfico Diario:")
    result = client.query("""
        SELECT 
            event_date,
            total_requests,
            unique_users,
            round(error_rate * 100, 2) as error_pct,
            round(suspicious_rate * 100, 2) as suspicious_pct
        FROM lakehouse.gold_daily_traffic_metrics
        ORDER BY event_date
    """)
    for row in result.result_rows:
        print(f"   {row[0]}: {row[1]} requests, {row[2]} users, {row[3]}% errors, {row[4]}% suspicious")
    
    # Insight 2: Top amenazas
    print("\n🔴 Top 5 IPs Más Amenazantes:")
    result = client.query("""
        SELECT 
            ip_address,
            ip_threat_type,
            total_requests,
            failed_logins,
            round(threat_score, 2) as score
        FROM lakehouse.gold_ip_threat_analysis
        ORDER BY threat_score DESC
        LIMIT 5
    """)
    for row in result.result_rows:
        print(f"   {row[0]} | {row[1]} | {row[2]} reqs | {row[3]} failed | score: {row[4]}")
    
    # Insight 3: Usuarios de alto riesgo
    print("\n👤 Usuarios con Mayor Riesgo Combinado:")
    result = client.query("""
        SELECT 
            username,
            user_role,
            total_requests,
            suspicious_events,
            round(combined_risk_score, 3) as risk
        FROM lakehouse.gold_user_activity_metrics
        ORDER BY combined_risk_score DESC
        LIMIT 5
    """)
    for row in result.result_rows:
        print(f"   {row[0]} ({row[1]}): {row[2]} reqs, {row[3]} suspicious, risk: {row[4]}")
    
    # Insight 4: Resumen de seguridad
    print("\n🛡️ Resumen de Seguridad:")
    result = client.query("""
        SELECT 
            summary_date,
            high_risk_events,
            brute_force_attempts,
            credential_stuffing_attempts,
            top_threat_type
        FROM lakehouse.gold_security_summary
        ORDER BY summary_date
    """)
    for row in result.result_rows:
        print(f"   {row[0]}: {row[1]} high-risk, {row[2]} brute-force, {row[3]} cred-stuffing, top: {row[4]}")

# =============================================================================
# FUNCIÓN PRINCIPAL
# =============================================================================

def run_gold_layer():
    """Ejecuta la creación y cálculo de la capa Gold."""
    print("\n" + "="*70)
    print("🥇 CAPA GOLD - MÉTRICAS Y KPIs DE NEGOCIO")
    print("="*70)
    
    # Conectar
    #client = conf.get_client()
    
    try:
        # Crear tablas Gold
        print("\n" + "-"*60)
        print("📋 CREANDO TABLAS GOLD")
        print("-"*60)
        create_gold_daily_traffic_table(client)
        create_gold_user_activity_table(client)
        create_gold_ip_threat_table(client)
        create_gold_security_summary_table(client)
        create_gold_hourly_patterns_table(client)
        
        # Calcular métricas
        print("\n" + "-"*60)
        print("📊 CALCULANDO MÉTRICAS")
        print("-"*60)
        calculate_daily_traffic_metrics(client)
        calculate_user_activity_metrics(client)
        calculate_ip_threat_analysis(client)
        calculate_security_summary(client)
        calculate_hourly_patterns(client)
        
        # Verificar y mostrar insights
        verify_gold_layer(client)
        show_gold_insights(client)
        
        print("\n" + "="*70)
        print("✅ CAPA GOLD COMPLETADA")
        print("="*70)
        
    except Exception as e:
        print(f"\n❌ Error: {e}")
        raise


if __name__ == "__main__":
    run_gold_layer()
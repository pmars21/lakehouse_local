"""
CAPA GOLD - VISTAS MATERIALIZADAS PARA KPIs Y ANALYTICS
========================================================

Este módulo crea vistas materializadas en ClickHouse para la capa Gold del Lakehouse.
Las vistas materializadas se actualizan automáticamente cuando llegan nuevos datos a Silver.

Categorías de KPIs:
1. Seguridad - Amenazas, IPs sospechosas, eventos anómalos
2. Rendimiento - Latencias, disponibilidad, throughput
3. Usuarios - Comportamiento por segmento, geografía, roles
4. Business Intelligence - Métricas ejecutivas consolidadas
"""

import clickhouse_connect
import time
import lakehouseConfig as conf


def create_gold_views():
    """
    Crea todas las vistas materializadas en la capa Gold.
    Las vistas materializadas ofrecen:
    - Consultas ultra-rápidas (datos pre-agregados)
    - Actualización automática cuando cambia Silver
    - Menor costo computacional en análisis repetitivos
    """
    client = conf.get_client()
    print("🥇 Iniciando creación de Capa GOLD...")
    start_time = time.time()

    # =========================================================================
    # CATEGORÍA 1: SEGURIDAD Y DETECCIÓN DE AMENAZAS
    # =========================================================================
    
    print("\n🔒 [1/4] Creando vistas de SEGURIDAD...")
    
    # 1.1 - Dashboard de Seguridad Diario
    # Agrega eventos sospechosos, IPs de riesgo y amenazas por día
    client.command("""
    CREATE MATERIALIZED VIEW IF NOT EXISTS gold.security_daily_summary
    ENGINE = SummingMergeTree()
    ORDER BY (event_date, ip_risk_level)
    POPULATE
    AS SELECT
        toDate(event_ts) AS event_date,
        ip_risk_level,
        ip_threat_type,
        
        -- Contadores de seguridad
        count() AS total_events,
        countIf(is_suspicious = 1) AS suspicious_events,
        countIf(status_code >= 400) AS error_events,
        countIf(status_code = 401 OR status_code = 403) AS auth_failures,
        
        -- IPs únicas por nivel de riesgo
        uniq(ip_address) AS unique_ips,
        
        -- Estadísticas de usuarios afectados
        uniq(user_id) AS unique_users_affected,
        countIf(user_is_premium = 1) AS premium_users_affected
        
    FROM silver.enriched_events
    GROUP BY event_date, ip_risk_level, ip_threat_type
    """)
    print("   ✅ security_daily_summary - Dashboard diario de seguridad")

    # 1.2 - Top IPs Maliciosas
    # Ranking de IPs más activas con comportamiento sospechoso
    client.command("""
    CREATE MATERIALIZED VIEW IF NOT EXISTS gold.top_malicious_ips
    ENGINE = AggregatingMergeTree()
    ORDER BY (ip_address, event_hour)
    POPULATE
    AS SELECT
        ip_address,
        toStartOfHour(event_ts) AS event_hour,
        ip_risk_level,
        ip_threat_type,
        ip_source,
        
        -- Métricas de actividad
        count() AS request_count,
        countIf(is_suspicious = 1) AS suspicious_count,
        countIf(status_code = 404) AS not_found_attempts,  -- Posible escaneo
        countIf(status_code >= 500) AS server_errors_caused,
        
        -- Diversidad de targets (posible ataque distribuido)
        uniq(url_path) AS unique_urls_accessed,
        uniq(user_id) AS unique_users_targeted,
        
        -- Promedio de tiempo de respuesta (puede indicar ataques DoS)
        avg(response_time_ms) AS avg_response_time
        
    FROM silver.enriched_events
    WHERE ip_risk_level IN ('high', 'critical', 'medium')
       OR is_suspicious = 1
    GROUP BY ip_address, event_hour, ip_risk_level, ip_threat_type, ip_source
    """)
    print("   ✅ top_malicious_ips - Ranking de IPs peligrosas por hora")

    # 1.3 - Alertas de Usuarios Comprometidos
    # Detecta usuarios con alto riesgo o comportamiento anómalo
    client.command("""
    CREATE MATERIALIZED VIEW IF NOT EXISTS gold.user_security_alerts
    ENGINE = ReplacingMergeTree()
    ORDER BY (user_id, alert_date)
    POPULATE
    AS SELECT
        user_id,
        user_name,
        user_email,
        user_country,
        toDate(event_ts) AS alert_date,
        
        -- Indicadores de compromiso
        countIf(ip_risk_level IN ('high', 'critical')) AS high_risk_ip_usage,
        countIf(is_suspicious = 1) AS suspicious_activities,
        countIf(status_code = 401) AS failed_auth_attempts,
        
        -- Diversidad geográfica sospechosa (múltiples IPs distintas)
        uniq(ip_address) AS distinct_ips_used,
        
        -- Actividad fuera de horario normal (simplificado)
        countIf(toHour(event_ts) < 6 OR toHour(event_ts) > 22) AS off_hours_activity,
        
        -- Score de riesgo calculado
        greatest(
            countIf(is_suspicious = 1) * 10 +
            countIf(ip_risk_level = 'critical') * 20 +
            countIf(ip_risk_level = 'high') * 10 +
            uniq(ip_address) * 2
        , 0) AS calculated_risk_score
        
    FROM silver.enriched_events
    WHERE user_id != ''
    GROUP BY user_id, user_name, user_email, user_country, alert_date
    HAVING calculated_risk_score > 50  -- Solo alertas significativas
    """)
    print("   ✅ user_security_alerts - Detección de usuarios comprometidos")

    # =========================================================================
    # CATEGORÍA 2: RENDIMIENTO Y DISPONIBILIDAD
    # =========================================================================
    
    print("\n⚡ [2/4] Creando vistas de RENDIMIENTO...")
    
    # 2.1 - SLA y Disponibilidad por Endpoint
    # Métricas de latencia y disponibilidad para cada URL
    client.command("""
    CREATE MATERIALIZED VIEW IF NOT EXISTS gold.endpoint_performance
    ENGINE = AggregatingMergeTree()
    ORDER BY (url_path, performance_hour)
    POPULATE
    AS SELECT
        url_path,
        http_method,
        toStartOfHour(event_ts) AS performance_hour,
        
        -- Volumen de tráfico
        count() AS total_requests,
        
        -- Códigos de estado (SLA)
        countIf(status_code >= 200 AND status_code < 300) AS success_count,
        countIf(status_code >= 400 AND status_code < 500) AS client_errors,
        countIf(status_code >= 500) AS server_errors,
        
        -- Disponibilidad (%)
        (countIf(status_code < 500) * 100.0) / count() AS availability_pct,
        
        -- Latencia (percentiles críticos para SLA)
        quantile(0.50)(response_time_ms) AS p50_latency_ms,
        quantile(0.95)(response_time_ms) AS p95_latency_ms,
        quantile(0.99)(response_time_ms) AS p99_latency_ms,
        avg(response_time_ms) AS avg_latency_ms,
        max(response_time_ms) AS max_latency_ms,
        
        -- Throughput
        sum(bytes_sent) AS total_bytes_sent,
        avg(bytes_sent) AS avg_bytes_per_request
        
    FROM silver.enriched_events
    GROUP BY url_path, http_method, performance_hour
    """)
    print("   ✅ endpoint_performance - SLA y latencias por endpoint")

    # 2.2 - Health Check Global por Hora
    # Vista consolidada del estado general del sistema
    client.command("""
    CREATE MATERIALIZED VIEW IF NOT EXISTS gold.system_health_hourly
    ENGINE = SummingMergeTree()
    ORDER BY health_hour
    POPULATE
    AS SELECT
        toStartOfHour(event_ts) AS health_hour,
        
        -- Volumen total
        count() AS total_requests,
        uniq(user_id) AS active_users,
        uniq(ip_address) AS unique_ips,
        
        -- Salud HTTP
        countIf(status_code = 200) AS http_200_ok,
        countIf(status_code >= 400 AND status_code < 500) AS http_4xx,
        countIf(status_code >= 500) AS http_5xx,
        
        -- Tasa de error global
        (countIf(status_code >= 500) * 100.0) / count() AS error_rate_pct,
        
        -- Performance global
        avg(response_time_ms) AS avg_response_time,
        quantile(0.95)(response_time_ms) AS p95_response_time,
        
        -- Ancho de banda
        sum(bytes_sent) / 1024 / 1024 AS total_mb_sent,  -- Convertido a MB
        
        -- Seguridad
        countIf(is_suspicious = 1) AS suspicious_events,
        countIf(ip_risk_level IN ('high', 'critical')) AS high_risk_events
        
    FROM silver.enriched_events
    GROUP BY health_hour
    """)
    print("   ✅ system_health_hourly - Salud del sistema hora a hora")

    # 2.3 - Análisis de Errores 5xx (Crítico para DevOps)
    # Detalla errores de servidor para troubleshooting
    client.command("""
    CREATE MATERIALIZED VIEW IF NOT EXISTS gold.server_errors_analysis
    ENGINE = ReplacingMergeTree()
    ORDER BY (error_hour, url_path, status_code)
    POPULATE
    AS SELECT
        toStartOfHour(event_ts) AS error_hour,
        url_path,
        http_method,
        status_code,
        
        -- Frecuencia del error
        count() AS error_count,
        
        -- Impacto en usuarios
        uniq(user_id) AS affected_users,
        uniq(ip_address) AS affected_ips,
        
        -- User agents afectados (útil para debugging)
        groupArray(5)(user_agent) AS sample_user_agents,
        
        -- Timing del error
        min(event_ts) AS first_occurrence,
        max(event_ts) AS last_occurrence,
        
        -- Performance en el momento del error
        avg(response_time_ms) AS avg_error_response_time
        
    FROM silver.enriched_events
    WHERE status_code >= 500
    GROUP BY error_hour, url_path, http_method, status_code
    """)
    print("   ✅ server_errors_analysis - Análisis detallado de errores 5xx")

    # =========================================================================
    # CATEGORÍA 3: ANÁLISIS DE USUARIOS
    # =========================================================================
    
    print("\n👥 [3/4] Creando vistas de USUARIOS...")
    
    # 3.1 - Segmentación de Usuarios (Premium vs Free)
    # Compara comportamiento entre segmentos de clientes
    client.command("""
    CREATE MATERIALIZED VIEW IF NOT EXISTS gold.user_segment_analytics
    ENGINE = SummingMergeTree()
    ORDER BY (analysis_date, user_is_premium, user_country)
    POPULATE
    AS SELECT
        toDate(event_ts) AS analysis_date,
        user_is_premium,
        user_country,
        user_role,
        
        -- Métricas de engagement
        uniq(user_id) AS unique_users,
        count() AS total_requests,
        
        -- Comportamiento de uso
        uniq(url_path) AS unique_pages_visited,
        countIf(http_method = 'POST') AS interactive_actions,  -- Acciones que modifican datos
        
        -- Performance percibida
        avg(response_time_ms) AS avg_perceived_latency,
        
        -- Calidad de servicio
        countIf(status_code >= 500) AS server_errors_encountered,
        (countIf(status_code < 400) * 100.0) / count() AS success_rate_pct,
        
        -- Seguridad
        countIf(is_suspicious = 1) AS suspicious_activities,
        countIf(ip_risk_level IN ('high', 'critical')) AS high_risk_sessions
        
    FROM silver.enriched_events
    WHERE user_id != ''
    GROUP BY analysis_date, user_is_premium, user_country, user_role
    """)
    print("   ✅ user_segment_analytics - Comparativa Premium vs Free")

    # 3.2 - Actividad Geográfica
    # Distribución de uso por país con métricas clave
    client.command("""
    CREATE MATERIALIZED VIEW IF NOT EXISTS gold.geographic_activity
    ENGINE = SummingMergeTree()
    ORDER BY (activity_date, user_country)
    POPULATE
    AS SELECT
        toDate(event_ts) AS activity_date,
        user_country,
        
        -- Volumen
        count() AS total_requests,
        uniq(user_id) AS unique_users,
        uniq(ip_address) AS unique_ips,
        
        -- Mix de usuarios
        countIf(user_is_premium = 1) AS premium_users,
        countIf(user_is_premium = 0) AS free_users,
        
        -- Performance regional
        avg(response_time_ms) AS avg_latency_ms,
        quantile(0.95)(response_time_ms) AS p95_latency_ms,
        
        -- Calidad de servicio regional
        countIf(status_code >= 500) AS server_errors,
        (countIf(status_code < 400) * 100.0) / count() AS success_rate_pct,
        
        -- Riesgos regionales
        countIf(is_suspicious = 1) AS suspicious_events,
        countIf(ip_risk_level IN ('high', 'critical')) AS high_risk_events,
        uniq(if(ip_risk_level IN ('high', 'critical'), ip_address, NULL)) AS risky_ips_count
        
    FROM silver.enriched_events
    WHERE user_country != '' AND user_country != 'XX'
    GROUP BY activity_date, user_country
    """)
    print("   ✅ geographic_activity - Métricas por país")

    # 3.3 - User Journey Analysis
    # Analiza paths de navegación y conversión
    client.command("""
    CREATE MATERIALIZED VIEW IF NOT EXISTS gold.user_journey_metrics
    ENGINE = AggregatingMergeTree()
    ORDER BY (journey_date, user_id)
    POPULATE
    AS SELECT
        user_id,
        user_name,
        user_role,
        user_is_premium,
        toDate(min(event_ts)) AS journey_date,
        
        -- Sesión
        count() AS page_views,
        uniq(url_path) AS unique_pages,
        
        -- Timeline
        min(event_ts) AS session_start,
        max(event_ts) AS session_end,
        dateDiff('minute', min(event_ts), max(event_ts)) AS session_duration_minutes,
        
        -- Camino del usuario (primeras 5 páginas visitadas)
        groupArray(5)(url_path) AS navigation_path,
        
        -- Engagement
        countIf(http_method = 'POST') AS actions_taken,
        countIf(status_code = 200) AS successful_loads,
        
        -- Fricción
        countIf(status_code = 404) AS not_found_errors,
        countIf(status_code >= 500) AS server_errors_faced,
        avg(response_time_ms) AS avg_load_time
        
    FROM silver.enriched_events
    WHERE user_id != ''
    GROUP BY user_id, user_name, user_role, user_is_premium
    """)
    print("   ✅ user_journey_metrics - Análisis de navegación por usuario")

    # =========================================================================
    # CATEGORÍA 4: BUSINESS INTELLIGENCE (KPIs EJECUTIVOS)
    # =========================================================================
    
    print("\n📊 [4/4] Creando vistas de BUSINESS INTELLIGENCE...")
    
    # 4.1 - Dashboard Ejecutivo Diario
    # Vista consolidada de todos los KPIs críticos del negocio
    client.command("""
    CREATE MATERIALIZED VIEW IF NOT EXISTS gold.executive_daily_kpis
    ENGINE = ReplacingMergeTree()
    ORDER BY kpi_date
    POPULATE
    AS SELECT
        toDate(event_ts) AS kpi_date,
        
        -- TRÁFICO
        count() AS total_requests,
        uniq(user_id) AS daily_active_users,
        uniq(ip_address) AS unique_visitors,
        
        -- SEGMENTACIÓN DE CLIENTES
        uniqIf(user_id, user_is_premium = 1) AS premium_active_users,
        uniqIf(user_id, user_is_premium = 0) AS free_active_users,
        (uniqIf(user_id, user_is_premium = 1) * 100.0) / uniq(user_id) AS premium_user_pct,
        
        -- ENGAGEMENT
        sum(bytes_sent) / 1024 / 1024 / 1024 AS total_gb_transferred,
        
        -- CALIDAD DE SERVICIO
        (countIf(status_code < 400) * 100.0) / count() AS overall_success_rate,
        avg(response_time_ms) AS avg_response_time,
        quantile(0.95)(response_time_ms) AS p95_response_time,
        
        -- SEGURIDAD (CRITICAL METRIC)
        countIf(is_suspicious = 1) AS total_suspicious_events,
        (countIf(is_suspicious = 1) * 100.0) / count() AS suspicious_event_rate,
        uniqIf(ip_address, ip_risk_level IN ('high', 'critical')) AS high_risk_ips,
        uniqIf(user_id, is_suspicious = 1) AS users_with_suspicious_activity,
        
        -- ERRORES
        countIf(status_code >= 500) AS server_errors,
        (countIf(status_code >= 500) * 100.0) / count() AS error_rate_pct,
        
        -- DISTRIBUCIÓN GEOGRÁFICA
        uniq(user_country) AS countries_served
        
    FROM silver.enriched_events
    GROUP BY kpi_date
    """)
    print("   ✅ executive_daily_kpis - Dashboard ejecutivo consolidado")

    # 4.2 - Revenue Proxy (Estimación de valor basada en engagement)
    # Aunque no hay datos de revenue, estimamos valor por engagement
    client.command("""
    CREATE MATERIALIZED VIEW IF NOT EXISTS gold.user_value_estimation
    ENGINE = SummingMergeTree()
    ORDER BY (value_date, user_id)
    POPULATE
    AS SELECT
        toDate(event_ts) AS value_date,
        user_id,
        user_name,
        user_is_premium,
        user_country,
        
        -- Métricas de engagement (proxy de valor)
        count() AS activity_score,  -- Más actividad = mayor valor
        uniq(toDate(event_ts)) AS days_active,
        countIf(http_method = 'POST') AS conversion_actions,
        
        -- Valor estimado (fórmula simplificada)
        -- Premium users valen más, más acciones valen más
        (
            count() * 1.0 +  -- Cada request = 1 punto
            countIf(http_method = 'POST') * 5.0 +  -- Cada acción = 5 puntos
            if(user_is_premium = 1, count() * 2.0, 0)  -- Premium users 3x
        ) AS estimated_value_points,
        
        -- Calidad del engagement
        (countIf(status_code < 400) * 100.0) / count() AS positive_experience_rate,
        avg(response_time_ms) AS avg_perceived_speed
        
    FROM silver.enriched_events
    WHERE user_id != ''
    GROUP BY value_date, user_id, user_name, user_is_premium, user_country
    """)
    print("   ✅ user_value_estimation - Estimación de valor por usuario")

    # 4.3 - Tendencias Semanales (Week-over-Week)
    # Compara métricas clave semana a semana
    client.command("""
    CREATE MATERIALIZED VIEW IF NOT EXISTS gold.weekly_trends
    ENGINE = SummingMergeTree()
    ORDER BY week_start
    POPULATE
    AS SELECT
        toMonday(event_ts) AS week_start,
        
        -- Crecimiento de usuarios
        uniq(user_id) AS weekly_active_users,
        uniq(ip_address) AS weekly_unique_visitors,
        
        -- Volumen
        count() AS total_requests,
        sum(bytes_sent) / 1024 / 1024 / 1024 AS total_gb_transferred,
        
        -- Calidad
        avg(response_time_ms) AS avg_response_time,
        (countIf(status_code < 400) * 100.0) / count() AS success_rate,
        
        -- Seguridad
        countIf(is_suspicious = 1) AS suspicious_events,
        uniqIf(ip_address, ip_risk_level IN ('high', 'critical')) AS risky_ips,
        
        -- Engagement premium
        uniqIf(user_id, user_is_premium = 1) AS premium_users,
        countIf(user_is_premium = 1) AS premium_requests,
        
        -- Mix geográfico
        uniq(user_country) AS countries_active
        
    FROM silver.enriched_events
    GROUP BY week_start
    """)
    print("   ✅ weekly_trends - Evolución semanal de KPIs")

    # =========================================================================
    # FINALIZACIÓN Y VERIFICACIÓN
    # =========================================================================
    
    duration = time.time() - start_time
    print("\n" + "="*60)
    print("✅ CAPA GOLD CREADA EXITOSAMENTE")
    print("="*60)
    print(f"⏱️  Tiempo total: {duration:.2f} segundos")
    print(f"\n📈 Vistas materializadas creadas:")
    print("\n🔒 SEGURIDAD (3 vistas):")
    print("   • security_daily_summary")
    print("   • top_malicious_ips")
    print("   • user_security_alerts")
    print("\n⚡ RENDIMIENTO (3 vistas):")
    print("   • endpoint_performance")
    print("   • system_health_hourly")
    print("   • server_errors_analysis")
    print("\n👥 USUARIOS (3 vistas):")
    print("   • user_segment_analytics")
    print("   • geographic_activity")
    print("   • user_journey_metrics")
    print("\n📊 BUSINESS INTELLIGENCE (3 vistas):")
    print("   • executive_daily_kpis")
    print("   • user_value_estimation")
    print("   • weekly_trends")
    print("\n" + "="*60)
    
    # Mostrar conteos de cada vista para verificación
    print("\n🔍 Verificando datos en vistas materializadas...")
    views = [
        'security_daily_summary', 'top_malicious_ips', 'user_security_alerts',
        'endpoint_performance', 'system_health_hourly', 'server_errors_analysis',
        'user_segment_analytics', 'geographic_activity', 'user_journey_metrics',
        'executive_daily_kpis', 'user_value_estimation', 'weekly_trends'
    ]
    
    for view in views:
        try:
            count = client.command(f"SELECT count() FROM gold.{view}")
            print(f"   ✓ gold.{view}: {count:,} registros")
        except Exception as e:
            print(f"   ✗ gold.{view}: Error - {e}")


def query_gold_examples():
    """
    Ejemplos de consultas útiles sobre las vistas Gold.
    Esta función muestra cómo consumir los datos de la capa Gold.
    """
    client = conf.get_client()
    print("\n" + "="*60)
    print("📊 EJEMPLOS DE CONSULTAS A CAPA GOLD")
    print("="*60)
    
    # Ejemplo 1: Top 5 países con más tráfico
    print("\n1️⃣ Top 5 países por volumen de tráfico:")
    result = client.query("""
        SELECT 
            user_country,
            sum(total_requests) as requests,
            sum(unique_users) as users,
            avg(success_rate_pct) as avg_success_rate
        FROM gold.geographic_activity
        WHERE activity_date >= today() - 7
        GROUP BY user_country
        ORDER BY requests DESC
        LIMIT 5
    """)
    print(result.result_rows)
    
    # Ejemplo 2: Alertas de seguridad de hoy
    print("\n2️⃣ Usuarios con alertas de seguridad recientes:")
    result = client.query("""
        SELECT 
            user_name,
            user_email,
            calculated_risk_score,
            suspicious_activities,
            high_risk_ip_usage
        FROM gold.user_security_alerts
        WHERE alert_date >= today() - 1
        ORDER BY calculated_risk_score DESC
        LIMIT 10
    """)
    print(result.result_rows)
    
    # Ejemplo 3: KPIs ejecutivos del día
    print("\n3️⃣ KPIs ejecutivos de hoy:")
    result = client.query("""
        SELECT 
            kpi_date,
            daily_active_users,
            premium_user_pct,
            overall_success_rate,
            avg_response_time,
            suspicious_event_rate
        FROM gold.executive_daily_kpis
        ORDER BY kpi_date DESC
        LIMIT 1
    """)
    print(result.result_rows)
    
    # Ejemplo 4: Endpoints más lentos
    print("\n4️⃣ Top 10 endpoints más lentos (última hora):")
    result = client.query("""
        SELECT 
            url_path,
            http_method,
            avg(avg_latency_ms) as avg_latency,
            sum(total_requests) as requests
        FROM gold.endpoint_performance
        WHERE performance_hour >= now() - INTERVAL 1 HOUR
        GROUP BY url_path, http_method
        ORDER BY avg_latency DESC
        LIMIT 10
    """)
    print(result.result_rows)


def run_gold_layer():
    """
    Función principal para ejecutar toda la capa Gold.
    """
    try:
        create_gold_views()
        # Descomentar para ver ejemplos de queries
        # query_gold_examples()
    except Exception as e:
        print(f"\n❌ Error en capa Gold: {e}")
        raise


if __name__ == "__main__":
    run_gold_layer()
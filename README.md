# 🏗️ Data Lakehouse - Web Security Analytics

## 📋 Resumen del Proyecto

Sistema de análisis de seguridad web implementado mediante una arquitectura **Data Lakehouse** de tres capas utilizando **ClickHouse** como motor analítico columnar y **MongoDB** como sistema operacional. El proyecto procesa logs de acceso web, información de usuarios y datos de reputación de IPs para detectar amenazas, analizar comportamiento de usuarios y generar métricas de negocio en tiempo real.

### 🎯 Objetivo

Implementar un pipeline de datos completo que permita:
- Ingestar datos desde múltiples fuentes (CSV, JSON)
- Almacenar datos raw sin transformación (Bronze)
- Enriquecer y limpiar datos mediante JOINs (Silver)
- Generar KPIs ejecutivos y vistas materializadas (Gold)
- Facilitar análisis de seguridad, rendimiento y comportamiento de usuarios

---

## 🏛️ Arquitectura de Tres Capas

```
┌─────────────────────────────────────────────────────────┐
│                    FUENTES DE DATOS                      │
├─────────────────┬──────────────────┬────────────────────┤
│  logs_web.csv   │   users.json     │ ip_reputation.json │
│   (Archivos)    │   (MongoDB)      │    (MongoDB)       │
└────────┬────────┴────────┬─────────┴──────────┬─────────┘
         │                 │                    │
         ▼                 ▼                    ▼
┌─────────────────────────────────────────────────────────┐
│         CAPA BRONZE (Raw Data - ClickHouse)              │
│  • logs_web: Eventos HTTP sin procesar                  │
│  • users: Información de usuarios                       │
│  • ip_reputation: Reputación de direcciones IP          │
│  • Todos los campos como String para flexibilidad       │
└─────────────────────────┬───────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────┐
│      CAPA SILVER (Clean & Enriched - ClickHouse)        │
│  • enriched_events: Tabla única con JOINs               │
│  • Tipado correcto (DateTime, Int32, Bool)              │
│  • Enriquecimiento: logs + usuarios + reputación IP     │
│  • Limpieza de nulos y valores por defecto              │
└─────────────────────────┬───────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────┐
│       CAPA GOLD (Analytics & KPIs - ClickHouse)         │
│  • 12 Vistas Materializadas pre-agregadas               │
│  • Seguridad: Alertas, IPs maliciosas, usuarios en riesgo│
│  • Rendimiento: SLA, latencias, health checks           │
│  • Usuarios: Segmentación, geografía, journeys          │
│  • Business Intelligence: KPIs ejecutivos, tendencias   │
└─────────────────────────────────────────────────────────┘
```

### 🗂️ Estructura del Proyecto

```
practica-final/
│
├── data/                         # 📁 Datos de entrada
│   ├── logs_web.csv              # Logs de acceso HTTP
│   ├── users.json                # Información de usuarios
│   └── ip_reputation.json        # Reputación de IPs (amenazas)
│
├── config.json                    #  Credenciales ClickHouse (MODIFICAR ESTE ARCHIVO CON LOS DATOS PROPIOS PARA PODER EJECUTAR EL PROYECTO)
├── config.py                      #  Configuración general (rutas, URIs) (MODIFICAR ESTE ARCHIVO CON LOS DATOS PROPIOS PARA PODER EJECUTAR EL PROYECTO)
│
├── lakehouseConfig.py            #  DDL: Creación de estructura del Lakehouse
├── mongo.py                      #  Carga de JSONs a MongoDB
├── bronze_layer.py               #  Ingesta a capa Bronze (Raw)
├── silver_layer.py               #  Transformación a capa Silver (Clean)
├── gold_layer.py                 #  Agregaciones a capa Gold (KPIs)
│
├── main.py                        #  Orquestador principal (ejecuta todo)
├── .gitignore                     # Ignora archivos sensibles
└── README.md                      # 📖 Esta documentación
```

---

## 🚀 Despliegue del Proyecto

### **Prerrequisitos**

| Tecnología | Versión Mínima | Propósito |
|------------|----------------|-----------|
| **Python** | 3.8+ | Lenguaje principal |
| **MongoDB** | 4.4+ | Base de datos operacional (staging) |
| **ClickHouse** | 22.0+ | Base de datos columnar OLAP |
| **pip** | Latest | Gestor de paquetes Python |

### **1. Instalación de Dependencias Python**

```bash
pip install pandas pymongo clickhouse-connect
```

**Librerías utilizadas:**
- `pandas`: Lectura y manipulación de CSV
- `pymongo`: Driver para conectar con MongoDB
- `clickhouse-connect`: Conector oficial de ClickHouse para Python

### **2. Configuración de MongoDB**

**Paso 1:** Levanta tu servicio de Mongo DB y actualiza la URI en `config.py` si usas credenciales o servidor remoto:

```python
db_uri = 'mongodb://localhost:27017/'
db_name = "practica_final_mongodb"
```

### **3. Configuración de ClickHouse**

**Paso 1:** Crea un archivo `config.json` en la raíz del proyecto con tus credenciales:

```json
{
    "host": "tu-host.clickhouse.cloud",
    "port": 8443,
    "username": "default",
    "password": "tu-password",
    "secure": true
}
```

**Ejemplo para ClickHouse local:**
```json
{
    "host": "localhost",
    "port": 8123,
    "username": "default",
    "password": "",
    "secure": false
}
```

**Paso 2:** Actualiza la ruta del config en `config.py`:

```python
config_file = r'C:\ruta\completa\al\config.json'
```

### **4. Preparación de Datos**

**Paso 1:** Coloca los archivos CSV y JSON en la carpeta `data/`:
- `logs_web.csv` (30 eventos HTTP)
- `users.json` (12 usuarios)
- `ip_reputation.json` (12 IPs con niveles de riesgo)

**Paso 2:** Actualiza la ruta de datos en `config.py`:

```python
ruta_data = r'C:\ruta\completa\a\la\carpeta\data'
```

### **5. Ejecución del Pipeline Completo**

```bash
python main.py
```

**Output esperado:**
```
==================================================
🚀 INICIANDO ORQUESTADOR DEL LAKEHOUSE
==================================================

[PASO 1/5] Cargando datos en MongoDB...
✅ Insertados 12 documentos en colección 'users'.
✅ Insertados 12 documentos en colección 'ip_reputation'.

[PASO 2/5] Creando estructura del Lakehouse...
✅ Base de datos 'bronze' lista.
✅ Base de datos 'silver' lista.
✅ Base de datos 'gold' lista.

[PASO 3/5] Ingestando Capa BRONZE...
✅ [logs_web] Ingestados 30 registros en Bronze.
✅ [users] Ingestados 12 usuarios desde Mongo.
✅ [ip_reputation] Ingestadas 12 IPs desde Mongo.

[PASO 4/5] Procesando Capa SILVER...
✅ Tabla 'silver.enriched_events' verificada.
✅ Registros generados: 27

[PASO 5/5] Ejecutando Capa GOLD...
✅ CAPA GOLD CREADA EXITOSAMENTE
📈 12 vistas materializadas creadas

==================================================
✅ EJECUCIÓN COMPLETADA CON ÉXITO
==================================================
```

### **6. Ejecución Modular (Opcional)**

Puedes ejecutar cada capa por separado para debugging:

```bash
# Solo carga a MongoDB
python mongo.py

# Solo crea estructura DDL
python lakehouseConfig.py

# Solo ingesta Bronze
python bronze_layer.py

# Solo procesa Silver
python silver_layer.py

# Solo genera Gold
python gold_layer.py
```

---

## 📄 Explicación de Scripts

### **1. `config.py` - Configuración Centralizada**

**Propósito:** Archivo de configuración que centraliza todas las rutas y credenciales del proyecto.

**Contenido:**
```python
# MongoDB
db_name = "practica_final_mongodb"
db_uri = 'mongodb://localhost:27017/'

# Rutas locales
ruta_data = r'C:\Users\...\data'
config_file = r'C:\Users\...\config.json'
```

**Por qué existe:** Facilita el cambio de rutas y credenciales sin modificar el código principal. Un único punto de actualización para todo el proyecto.

---

### **2. `lakehouseConfig.py` - Estructura DDL del Lakehouse**

**Propósito:** Define la arquitectura de tres capas en ClickHouse mediante DDL (Data Definition Language).

**Funciones principales:**

#### `get_client()`
```python
def get_client():
    with open(conf.config_file, 'r') as file:
        config = json.load(file)
    client = clickhouse_connect.get_client(
        host=config["host"],
        port=config["port"],
        username=config["username"],
        password=config["password"],
        secure=config["secure"]
    )
    return client
```
**Qué hace:** Lee las credenciales del `config.json` y devuelve un cliente conectado a ClickHouse que otros módulos reutilizan.

#### `setup_lakehouse()`
**Qué hace:**
1. **Crea 3 bases de datos:** `bronze`, `silver`, `gold`
2. **Crea tablas en Bronze:**
   - `bronze.logs_web`: 11 columnas (event_id, event_ts, user_id, ip_address, http_method, url_path, status_code, bytes_sent, response_time_ms, user_agent, is_suspicious)
   - `bronze.users`: 8 columnas (_id, username, email, role, country, created_at, is_premium, risk_score)
   - `bronze.ip_reputation`: 5 columnas (ip, source, risk_level, threat_type, last_seen)

**Decisión de diseño:** Todas las columnas en Bronze son `String` para maximizar flexibilidad en la ingesta. No queremos que falle por un formato inesperado. La conversión de tipos se hace después en Silver.

**Motor usado:** `MergeTree()` - Motor columnar optimizado de ClickHouse para OLAP.

---

### **3. `mongo.py` - Carga de Datos Operacionales**

**Propósito:** Ingesta los archivos JSON (`users.json`, `ip_reputation.json`) a MongoDB como paso intermedio.

**Funciones principales:**

#### `create_mongo_connection()`
```python
def create_mongo_connection():
    client = MongoClient(conf.db_uri)
    db = client[conf.db_name]
    return client, db
```
**Qué hace:** Establece conexión con MongoDB y devuelve el cliente y la base de datos para trabajar con colecciones.

#### `load_data_to_mongo()`
**Proceso paso a paso:**

1. **Lee `users.json` del disco:**
   ```python
   with open(path_users, 'r', encoding='utf-8') as f:
       users_data = json.load(f)
   ```

2. **Limpia la colección anterior (idempotencia):**
   ```python
   db.users.drop()
   ```
   Esto permite re-ejecutar el script sin duplicar datos.

3. **Inserta todos los documentos:**
   ```python
   db.users.insert_many(users_data)
   ```

4. **Repite el mismo proceso para `ip_reputation.json`**

**Por qué MongoDB:** Aunque ClickHouse podría leer JSON directamente, MongoDB sirve como "staging area" operacional. En un sistema real, estos datos vendrían de sistemas transaccionales que usan MongoDB.

---

### **4. `bronze_layer.py` - Ingesta a Capa Raw**

**Propósito:** Ingesta datos desde múltiples fuentes (CSV y MongoDB) hacia la capa Bronze de ClickHouse sin transformaciones.

**Función principal:** `ingest_bronze()`

**Proceso de ingesta:**

#### **A. Logs Web (CSV → ClickHouse)**
```python
df_logs = pd.read_csv(path_logs_csv, dtype=str)
df_logs = df_logs.fillna('')
ch_client.insert_df('bronze.logs_web', df_logs)
```

**Qué hace:**
1. Lee el CSV completo con pandas
2. Fuerza todos los campos a `String` con `dtype=str`
3. Rellena valores nulos con cadena vacía
4. Inserta el DataFrame directamente en ClickHouse usando `insert_df()`

**Por qué `dtype=str`:** Evita problemas de tipado. Si un campo numérico tiene un valor "N/A" en el CSV, pandas no falla porque lo trata como string.

#### **B. Usuarios (MongoDB → ClickHouse)**
```python
cursor_users = mongo_db.users.find({})
users_list = list(cursor_users)

for doc in users_list:
    row = [
        str(doc.get('_id', '')),
        str(doc.get('username', '')),
        ...
    ]
    data_to_insert.append(row)

ch_client.insert('bronze.users', data_to_insert, column_names=column_names)
```

**Qué hace:**
1. Recupera todos los documentos de la colección `users`
2. Convierte cada documento en una lista de strings
3. Inserta usando el método `insert()` especificando nombres de columnas

**Detalle importante:** Los booleanos de MongoDB (`is_premium: true`) se convierten a strings (`"True"`) para coincidir con la definición de Bronze.

#### **C. IP Reputation (MongoDB → ClickHouse)**
Mismo proceso que usuarios pero para la colección `ip_reputation`.

**Resultado:** 3 tablas Bronze pobladas con datos raw sin transformaciones.

---

### **5. `silver_layer.py` - Transformación y Enriquecimiento**

**Propósito:** Crear una tabla única enriquecida mediante JOINs, con tipos de datos correctos y valores limpios.

**Función principal:** `process_silver()`

**Proceso de transformación:**

#### **1. Definición de la tabla Silver**
```sql
CREATE TABLE silver.enriched_events (
    -- Datos del Log (11 campos)
    event_id String,
    event_ts DateTime,  -- ¡Convertido de String!
    user_id String,
    ip_address String,
    http_method String,
    url_path String,
    status_code Int32,  -- ¡Convertido de String!
    bytes_sent Int32,
    response_time_ms Int32,
    user_agent String,
    is_suspicious UInt8,  -- ¡Convertido a booleano!
    
    -- Datos Enriquecidos del Usuario (5 campos)
    user_name String,
    user_email String,
    user_role String,
    user_country String,
    user_is_premium Bool,
    
    -- Datos Enriquecidos de IP (3 campos)
    ip_risk_level String,
    ip_threat_type String,
    ip_source String
) ENGINE = MergeTree()
ORDER BY event_ts
```

**Total:** 19 campos (11 logs + 5 usuarios + 3 reputación IP)

#### **2. Query ETL con JOINs**
```sql
INSERT INTO silver.enriched_events
SELECT
    -- Logs (con conversiones de tipo)
    L.event_id,
    parseDateTimeBestEffort(L.event_ts) AS event_ts,
    L.user_id,
    L.ip_address,
    ...
    ifNull(L.bytes_sent, 0),  -- Limpieza de nulos
    
    -- JOIN con Users (LEFT JOIN porque pueden haber logs anónimos)
    if(U.username = '', 'Anonymous', U.username) as user_name,
    U.email,
    if(U.role = '', 'guest', U.role) as user_role,
    if(U.country = '', 'XX', U.country) as user_country,
    ifNull(U.is_premium, 0) as user_is_premium,
    
    -- JOIN con IP Reputation
    if(I.risk_level = '', 'unknown', I.risk_level) as ip_risk_level,
    if(I.threat_type = '', 'benign', I.threat_type) as ip_threat_type,
    I.source

FROM bronze.logs_web AS L
LEFT JOIN bronze.users AS U ON L.user_id = U._id
LEFT JOIN bronze.ip_reputation AS I ON L.ip_address = I.ip
WHERE L.user_id IS NOT NULL AND L.user_id != ''
```

**Transformaciones aplicadas:**

1. **Conversión de tipos:**
   - `event_ts`: String → DateTime usando `parseDateTimeBestEffort()`
   - `status_code`, `bytes_sent`, `response_time_ms`: String → Int32
   - `is_suspicious`: String → UInt8 (0 o 1)

2. **Limpieza de nulos:**
   - `ifNull(L.bytes_sent, 0)`: Convierte NULL a 0
   - Valores por defecto: 'Anonymous', 'guest', 'XX', 'unknown', 'benign'

3. **LEFT JOINs:**
   - **¿Por qué LEFT JOIN?** Porque pueden existir:
     - Logs de usuarios no registrados (user_id vacío)
     - IPs que no están en nuestra base de reputación
   - **Con LEFT JOIN** no perdemos logs, solo quedan con valores por defecto

**Resultado:** Tabla `silver.enriched_events` con 27 registros (el filtro `WHERE user_id IS NOT NULL` elimina 3 logs anónimos de los 30 originales).

---

### **6. `gold_layer.py` - Vistas Materializadas para Analytics**

**Propósito:** Crear 12 vistas materializadas pre-agregadas que responden preguntas de negocio en milisegundos.

**Función principal:** `create_gold_views()`

**Categorías de KPIs (12 vistas en total):**

#### **🔒 1. SEGURIDAD (3 vistas)**

##### **1.1. `security_daily_summary`**

**Pregunta que responde:** "¿Cuántos eventos sospechosos tuvimos hoy por nivel de riesgo de IP?"

**Motor:** `SummingMergeTree()` - Suma automáticamente valores cuando se insertan datos con la misma clave (event_date, ip_risk_level).

##### **1.2. `top_malicious_ips`**
**Pregunta:** "¿Cuáles son las IPs más activas con comportamiento malicioso?"

**Métricas:**
- Conteo de requests sospechosos
- Intentos de acceso a páginas 404 (posible escaneo)
- URLs únicas accedidas (indica ataque distribuido)
- Promedio de tiempo de respuesta (puede indicar DoS)

##### **1.3. `user_security_alerts`**
**Pregunta:** "¿Qué usuarios muestran señales de compromiso?"

**Indicadores de riesgo calculados:**
```sql
calculated_risk_score = 
    countIf(is_suspicious = 1) * 10 +
    countIf(ip_risk_level = 'critical') * 20 +
    countIf(ip_risk_level = 'high') * 10 +
    uniq(ip_address) * 2
```

**Filtro:** Solo muestra usuarios con score > 50 (alertas significativas).

#### **⚡ 2. RENDIMIENTO (3 vistas)**

##### **2.1. `endpoint_performance`**
**Pregunta:** "¿Cuál es el SLA y latencia de cada endpoint?"

**Métricas clave:**
```sql
quantile(0.50)(response_time_ms) AS p50_latency_ms,  -- Mediana
quantile(0.95)(response_time_ms) AS p95_latency_ms,  -- P95 (SLA típico)
quantile(0.99)(response_time_ms) AS p99_latency_ms,  -- P99 (worst case)
(countIf(status_code < 500) * 100.0) / count() AS availability_pct
```

**Uso en producción:** Detectar endpoints lentos o con alta tasa de error.

##### **2.2. `system_health_hourly`**
**Pregunta:** "¿Cuál es la salud general del sistema hora a hora?"

**Snapshot cada hora:**
- Requests totales, usuarios activos, IPs únicas
- Tasa de error global: `(countIf(status_code >= 500) * 100.0) / count()`
- Ancho de banda: `sum(bytes_sent) / 1024 / 1024` (convertido a MB)
- Eventos de seguridad

##### **2.3. `server_errors_analysis`**
**Pregunta:** "¿Qué errores 5xx están ocurriendo y por qué?"

**Detalles para debugging:**
- Hora exacta del primer y último error
- User agents afectados (para identificar si es un cliente específico)
- Usuarios y IPs impactados

#### **👥 3. USUARIOS (3 vistas)**

##### **3.1. `user_segment_analytics`**
**Pregunta:** "¿Cómo se comportan usuarios Premium vs Free?"

**Comparativa:**
```sql
GROUP BY analysis_date, user_is_premium, user_country, user_role
```

**Métricas:**
- Engagement: requests totales, páginas únicas visitadas
- Acciones interactivas: `countIf(http_method = 'POST')`
- Calidad de servicio percibida: latencia promedio, tasa de éxito
- Seguridad: actividades sospechosas por segmento

##### **3.2. `geographic_activity`**
**Pregunta:** "¿Cómo varía el uso y rendimiento por país?"

**Distribución geográfica:**
- Volumen por país
- Mix Premium/Free por región
- Performance regional (latencias P50, P95)
- Riesgos regionales (IPs de alto riesgo por país)

##### **3.3. `user_journey_metrics`**
**Pregunta:** "¿Cómo navegan los usuarios por la aplicación?"

**Path de navegación:**
```sql
groupArray(5)(url_path) AS navigation_path  -- Primeras 5 páginas
dateDiff('minute', min(event_ts), max(event_ts)) AS session_duration_minutes
```

**Fricción detectada:**
- Errores 404 (páginas no encontradas)
- Errores 5xx enfrentados por el usuario
- Tiempo de carga promedio

#### **📊 4. BUSINESS INTELLIGENCE (3 vistas)**

##### **4.1. `executive_daily_kpis`**
**Pregunta:** "¿Cuáles son los KPIs ejecutivos del día?"

**Audiencia:** C-level executives (CEO, CTO, CISO)

##### **4.2. `user_value_estimation`**
**Pregunta:** "¿Qué usuarios generan más valor?"

**Modelo de valor (sin datos de revenue):**
```sql
estimated_value_points = 
    count() * 1.0 +                              -- Cada request = 1 punto
    countIf(http_method = 'POST') * 5.0 +        -- Cada acción = 5 puntos
    if(user_is_premium = 1, count() * 2.0, 0)    -- Premium users 3x
```

**Uso:** Identificar usuarios high-value para retención o upselling.

##### **4.3. `weekly_trends`**
**Pregunta:** "¿Cómo evolucionan las métricas semana a semana?"

**Tendencias tracked:**
```sql
toMonday(event_ts) AS week_start  -- Agrupa por inicio de semana
```

**Métricas de crecimiento:**
- Usuarios activos semanales (WAU)
- Volumen de requests
- Tasa de éxito y seguridad
- Mix de usuarios premium

**Uso:** Detectar tendencias positivas/negativas, estacionalidad.

---

### **7. `main.py` - Orquestador del Pipeline**

**Propósito:** Ejecutar el pipeline completo en el orden correcto con manejo de errores.

**Proyecto desarrollado como práctica final de Gestión de Almacenamiento y Big Data**
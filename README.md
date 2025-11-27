# 🏗️ Data Lakehouse - Web Security Analytics

## 📋 Descripción del Proyecto

Sistema de análisis de seguridad web implementado mediante una arquitectura **Data Lakehouse** de tres capas (Bronze, Silver, Gold) utilizando **ClickHouse** como motor analítico y **MongoDB** como sistema operacional. El proyecto procesa logs web, información de usuarios y reputación de IPs para detectar patrones de actividad sospechosa y generar métricas de negocio.

### 🎯 Objetivo

Implementar un pipeline de datos completo que permita:
- Ingestar datos desde múltiples fuentes (CSV, JSON)
- Almacenar datos raw en capa Bronze
- Transformar y limpiar datos en capa Silver
- Generar KPIs y agregaciones en capa Gold
- Facilitar análisis de seguridad y patrones de acceso web

---

## 🏛️ Arquitectura del Sistema

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
│            CAPA BRONZE (ClickHouse)                      │
│  • logs_web          • users        • ip_reputation     │
│  • Datos Raw (String) sin transformación                │
└─────────────────────────┬───────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────┐
│            CAPA SILVER (ClickHouse)                      │
│  • Datos limpios y tipados                              │
│  • Joins y enriquecimiento                              │
│  • Validaciones de calidad                              │
└─────────────────────────┬───────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────┐
│             CAPA GOLD (ClickHouse)                       │
│  • KPIs de negocio                                       │
│  • Agregaciones y métricas                              │
│  • Tablas optimizadas para reporting                    │
└─────────────────────────────────────────────────────────┘
```

---

## 🗂️ Estructura del Proyecto

```
practica-final/
│
├── data/                          # Datos de entrada
│   ├── logs_web.csv              # Logs de acceso web
│   ├── users.json                # Información de usuarios
│   └── ip_reputation.json        # Reputación de IPs
│
├── config.json                    # Credenciales ClickHouse
│
├── config.py                      # Configuración general del proyecto
├── mongo.py                       # Carga de datos a MongoDB
├── lakehouseConfig.py            # Configuración y DDL de ClickHouse
├── bronze_layer.py               # Ingesta a capa Bronze
├── main.py                        # Orquestador principal
│
└── README.md                      # Documentación del proyecto
```

---

## 📊 Modelo de Datos

### **Capa Bronze**

#### 1. `bronze.logs_web`
Logs de acceso web en formato raw.

| Campo | Tipo | Descripción |
|-------|------|-------------|
| `event_id` | String | Identificador único del evento |
| `event_ts` | String | Timestamp del evento |
| `user_id` | String | ID del usuario |
| `ip_address` | String | Dirección IP del cliente |
| `http_method` | String | Método HTTP (GET, POST, etc.) |
| `url_path` | String | Ruta de la URL solicitada |
| `status_code` | String | Código de respuesta HTTP |
| `bytes_sent` | String | Bytes enviados en la respuesta |
| `response_time_ms` | String | Tiempo de respuesta en ms |
| `user_agent` | String | User agent del navegador |
| `is_suspicious` | String | Indicador de actividad sospechosa |

#### 2. `bronze.users`
Información de usuarios del sistema.

| Campo | Tipo | Descripción |
|-------|------|-------------|
| `_id` | String | Identificador único del usuario |
| `username` | String | Nombre de usuario |
| `email` | String | Correo electrónico |
| `role` | String | Rol del usuario |
| `country` | String | País del usuario |
| `created_at` | String | Fecha de creación |
| `is_premium` | String | Indicador de usuario premium |
| `risk_score` | String | Puntuación de riesgo |

#### 3. `bronze.ip_reputation`
Reputación de direcciones IP conocidas.

| Campo | Tipo | Descripción |
|-------|------|-------------|
| `ip` | String | Dirección IP |
| `source` | String | Fuente de la información |
| `risk_level` | String | Nivel de riesgo |
| `threat_type` | String | Tipo de amenaza |
| `last_seen` | String | Última vez detectada |

---

## ⚙️ Configuración e Instalación

### **Prerrequisitos**

- Python 3.8+
- MongoDB 4.4+
- ClickHouse 22.0+
- Docker (opcional, para ejecutar ClickHouse/MongoDB)

### **Dependencias de Python**

```bash
pip install pandas pymongo clickhouse-connect
```

### **1. Configuración de MongoDB**

Asegúrate de que MongoDB esté ejecutándose en `localhost:27017` o actualiza la URI en `config.py`:

```python
db_uri = 'mongodb://localhost:27017/'
db_name = "practica_final_mongodb"
```

### **2. Configuración de ClickHouse**

Crea un archivo `config.json` con tus credenciales:

```json
{
    "host": "localhost",
    "port": 8123,
    "username": "default",
    "password": "",
    "secure": false
}
```

Actualiza la ruta en `config.py`:

```python
config_file = r'C:\ruta\a\tu\config.json'
```

### **3. Preparación de Datos**

Coloca los archivos de datos en la carpeta especificada y actualiza la ruta en `config.py`:

```python
ruta_data = r'C:\ruta\a\tu\carpeta\data'
```

Archivos requeridos:
- `logs_web.csv`
- `users.json`
- `ip_reputation.json`

---

## 🚀 Ejecución del Proyecto

### **Ejecución Completa**

Ejecuta el orquestador principal que procesa todas las capas:

```bash
python main.py
```

### **Ejecución por Módulos**

También puedes ejecutar cada módulo de forma independiente:

#### 1. Cargar datos a MongoDB
```bash
python mongo.py
```

#### 2. Crear estructura del Lakehouse
```bash
python lakehouseConfig.py
```

#### 3. Ingestar datos a capa Bronze
```bash
python bronze_layer.py
```

---

## 📝 Descripción de Módulos

### **1. `config.py`**
**Propósito:** Centralizar todas las configuraciones del proyecto.

**Contenido:**
- Credenciales de MongoDB
- Rutas de datos
- Ruta al archivo de configuración de ClickHouse

### **2. `mongo.py`**
**Propósito:** Gestionar la carga inicial de datos JSON a MongoDB.

**Funciones principales:**
- `create_mongo_connection()`: Establece conexión con MongoDB
- `load_data_to_mongo()`: Carga `users.json` e `ip_reputation.json` a sus respectivas colecciones

**Proceso:**
1. Conecta a MongoDB
2. Limpia colecciones existentes (drop)
3. Lee archivos JSON desde disco
4. Inserta documentos en MongoDB usando `insert_many()`

### **3. `lakehouseConfig.py`**
**Propósito:** Configurar la estructura del Data Lakehouse en ClickHouse.

**Funciones principales:**
- `get_client()`: Obtiene cliente de ClickHouse con credenciales del config.json
- `setup_lakehouse()`: Crea las bases de datos y tablas necesarias

**Proceso:**
1. Crea bases de datos: `bronze`, `silver`, `gold`
2. Define tablas en Bronze con motor MergeTree
3. Todas las columnas en Bronze son tipo String para flexibilidad en la ingesta raw

**Tablas creadas:**
- `bronze.logs_web`
- `bronze.users`
- `bronze.ip_reputation`

### **4. `bronze_layer.py`**
**Propósito:** Ingestar datos desde las fuentes a la capa Bronze de ClickHouse.

**Función principal:**
- `ingest_bronze()`: Orquesta toda la ingesta a Bronze

**Proceso:**
1. **Logs Web (CSV → ClickHouse):**
   - Lee `logs_web.csv` usando pandas
   - Convierte todo a string y rellena NaNs
   - Inserta en `bronze.logs_web` usando `insert_df()`

2. **Users (MongoDB → ClickHouse):**
   - Lee documentos de la colección `users`
   - Convierte campos a string
   - Inserta en `bronze.users` usando `insert()`

3. **IP Reputation (MongoDB → ClickHouse):**
   - Lee documentos de la colección `ip_reputation`
   - Convierte campos a string
   - Inserta en `bronze.ip_reputation` usando `insert()`

**Consideraciones técnicas:**
- Usa `dtype=str` en pandas para evitar problemas de tipos
- `fillna('')` para manejar valores nulos
- Conversión explícita a string para booleanos y números

### **5. `main.py`**
**Propósito:** Orquestador principal que ejecuta el pipeline completo.

**Flujo de ejecución:**
```
PASO 1: Carga datos a MongoDB (mongo.py)
    ↓
PASO 2: Crea estructura Lakehouse (lakehouseConfig.py)
    ↓
PASO 3: Ingesta a Bronze (bronze_layer.py)
    ↓
PASO 4: Procesa Silver [🚧 Pendiente]
    ↓
PASO 5: Calcula Gold KPIs [🚧 Pendiente]
```

**Características:**
- Control de errores con try-except
- `sys.exit(1)` si falla algún paso crítico
- Mensajes informativos con emojis para mejor UX

---

## 🔍 Casos de Uso

### **1. Análisis de Seguridad**
- Detección de IPs sospechosas mediante cruce con tabla de reputación
- Identificación de patrones de acceso anómalos
- Análisis de usuarios con alto `risk_score`

### **2. Métricas de Rendimiento**
- Tiempos de respuesta promedio por endpoint
- Distribución de códigos de estado HTTP
- Volumen de tráfico por usuario/país

### **3. Análisis de Usuarios**
- Segmentación por tipo de usuario (premium vs. free)
- Distribución geográfica
- Patrones de uso por rol

---

## 🛠️ Tecnologías Utilizadas

| Tecnología | Propósito | Versión |
|------------|-----------|---------|
| **Python** | Lenguaje principal | 3.8+ |
| **ClickHouse** | Base de datos columnar OLAP | 22.0+ |
| **MongoDB** | Base de datos operacional NoSQL | 4.4+ |
| **Pandas** | Procesamiento de datos | Latest |
| **clickhouse-connect** | Conector Python para ClickHouse | Latest |
| **PyMongo** | Driver Python para MongoDB | Latest |

---

## 📈 Próximos Pasos

### **Capa Silver** 🚧
- [ ] Conversión de tipos de datos (String → DateTime, Int, Float)
- [ ] Limpieza y validación de datos
- [ ] Join entre logs, users e ip_reputation
- [ ] Detección y filtrado de registros anómalos
- [ ] Cálculo de columnas derivadas

### **Capa Gold** 🚧
- [ ] KPIs de seguridad (eventos sospechosos por día/usuario)
- [ ] Métricas de rendimiento (latencia P50, P95, P99)
- [ ] Agregaciones por dimensiones (país, rol, IP)
- [ ] Tablas materializadas para dashboards
- [ ] Cálculo de trends temporales

### **Mejoras Adicionales**
- [ ] Implementar logging estructurado
- [ ] Añadir tests unitarios
- [ ] Dockerización completa del proyecto
- [ ] Pipeline CI/CD
- [ ] Monitoreo y alertas
- [ ] Documentación API

---

## 🐛 Troubleshooting

### **Error: No se puede conectar a MongoDB**
```
Solución: Verifica que MongoDB esté corriendo:
- Windows: Revisa servicios de Windows
- Linux/Mac: sudo systemctl status mongod
```

### **Error: ClickHouse connection refused**
```
Solución: Verifica que ClickHouse esté corriendo en el puerto 8123
- Revisa config.json tenga las credenciales correctas
- Prueba acceder a http://localhost:8123 en el navegador
```

### **Error: Archivo CSV no encontrado**
```
Solución: Actualiza la ruta en config.py:
ruta_data = r'C:\tu\ruta\correcta\data'
```

--

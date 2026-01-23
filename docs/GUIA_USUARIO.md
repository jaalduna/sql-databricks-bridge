# Guía de Usuario - SQL-Databricks Bridge

## Tabla de Contenidos

1. [Requisitos Previos](#requisitos-previos)
2. [Instalación](#instalación)
3. [Configuración de Variables de Entorno](#configuración-de-variables-de-entorno)
4. [Configuración de Queries Parametrizadas](#configuración-de-queries-parametrizadas)
5. [Uso del CLI](#uso-del-cli)
6. [Uso de la API REST](#uso-de-la-api-rest)
7. [Sincronización Databricks → SQL Server](#sincronización-databricks--sql-server)
8. [Solución de Problemas](#solución-de-problemas)

---

## Requisitos Previos

- Python 3.11 o superior
- Poetry (gestor de dependencias)
- Acceso a SQL Server (opcional para desarrollo)
- Acceso a Databricks con token o Service Principal

---

## Instalación

```bash
# Clonar el repositorio
git clone https://github.com/jaalduna/sql-databricks-bridge.git
cd sql-databricks-bridge

# Instalar dependencias
poetry install

# Copiar archivo de ejemplo de variables de entorno
cp .env.example .env
```

---

## Configuración de Variables de Entorno

### Opción 1: Archivo `.env` (Recomendado para desarrollo local)

Edita el archivo `.env` en la raíz del proyecto:

```bash
# ===========================================
# SQL SERVER - Conexión a base de datos origen
# ===========================================
SQLSERVER_HOST=tu-servidor.database.windows.net
SQLSERVER_PORT=1433
SQLSERVER_DATABASE=tu_base_de_datos
SQLSERVER_USERNAME=tu_usuario
SQLSERVER_PASSWORD=tu_contraseña
SQLSERVER_DRIVER=ODBC Driver 18 for SQL Server
SQLSERVER_TRUST_CERT=true

# ===========================================
# DATABRICKS - Conexión al workspace
# ===========================================
# Host del workspace (sin trailing slash)
DATABRICKS_HOST=https://adb-1234567890.azuredatabricks.net

# Autenticación por Token (opción más simple)
DATABRICKS_TOKEN=dapi1234567890abcdef

# O Autenticación por Service Principal (para producción)
# DATABRICKS_CLIENT_ID=tu-client-id
# DATABRICKS_CLIENT_SECRET=tu-client-secret

# SQL Warehouse ID (requerido para ejecutar queries)
DATABRICKS_WAREHOUSE_ID=abc123def456

# Unity Catalog
DATABRICKS_CATALOG=mi_catalogo
DATABRICKS_SCHEMA=mi_schema
DATABRICKS_VOLUME=mi_volume

# ===========================================
# APLICACIÓN - Configuración del servicio
# ===========================================
ENVIRONMENT=development
LOG_LEVEL=INFO
AUTH_ENABLED=false

# Archivo de permisos (para control de acceso)
PERMISSIONS_FILE=config/permissions.yaml
PERMISSIONS_HOT_RELOAD=true
```

### Opción 2: Variables de Entorno del Sistema

Si prefieres no usar `.env`, exporta las variables en tu terminal o agrégalas a `~/.bashrc`:

```bash
# Agregar a ~/.bashrc o ~/.zshrc
export DATABRICKS_HOST="https://adb-1234567890.azuredatabricks.net"
export DATABRICKS_TOKEN="dapi1234567890abcdef"
export DATABRICKS_WAREHOUSE_ID="abc123def456"
export DATABRICKS_CATALOG="mi_catalogo"
export DATABRICKS_SCHEMA="mi_schema"

export SQLSERVER_HOST="tu-servidor.database.windows.net"
export SQLSERVER_DATABASE="tu_base_de_datos"
export SQLSERVER_USERNAME="tu_usuario"
export SQLSERVER_PASSWORD="tu_contraseña"

# Recargar configuración
source ~/.bashrc
```

### Verificar Configuración

```bash
# Verificar que las variables están configuradas
poetry run python -c "from sql_databricks_bridge.core.config import get_settings; s = get_settings(); print(f'Databricks Host: {s.databricks.host}')"

# Probar conexión a Databricks
poetry run python scripts/test_databricks_connection.py
```

---

## Configuración de Queries Parametrizadas

### Estructura de Directorios

```
mi_proyecto/
├── queries/                    # Archivos SQL con parámetros
│   ├── extraer_usuarios.sql
│   ├── extraer_ventas.sql
│   └── extraer_productos.sql
└── config/                     # Configuración de parámetros
    ├── common_params.yaml      # Parámetros compartidos
    ├── CL.yaml                 # Parámetros para Chile
    ├── AR.yaml                 # Parámetros para Argentina
    └── CO.yaml                 # Parámetros para Colombia
```

### Sintaxis de Parámetros en SQL

Usa `{nombre_parametro}` para definir parámetros:

**queries/extraer_usuarios.sql**
```sql
-- Extrae usuarios activos por país
SELECT
    usuario_id,
    nombre,
    email,
    fecha_registro
FROM {database}.{schema}.usuarios
WHERE pais = '{codigo_pais}'
  AND estado = 'activo'
  AND fecha_registro >= '{fecha_inicio}'
  AND fecha_registro < '{fecha_fin}'
ORDER BY fecha_registro DESC
```

**queries/extraer_ventas.sql**
```sql
-- Extrae ventas del período
SELECT
    v.venta_id,
    v.usuario_id,
    v.producto_id,
    v.cantidad,
    v.monto_total,
    v.fecha_venta
FROM {database}.{schema}.ventas v
INNER JOIN {database}.{schema}.usuarios u
    ON v.usuario_id = u.usuario_id
WHERE u.pais = '{codigo_pais}'
  AND v.fecha_venta >= '{fecha_inicio}'
  AND v.fecha_venta < '{fecha_fin}'
```

### Archivos de Configuración YAML

**config/common_params.yaml** (valores por defecto)
```yaml
# Conexión a base de datos
database: kpi_warehouse
schema: dbo

# Fechas por defecto
fecha_inicio: "2024-01-01"
fecha_fin: "2024-12-31"

# Configuración de extracción
chunk_size: 100000
```

**config/CL.yaml** (específico para Chile)
```yaml
codigo_pais: CL
nombre_pais: Chile

# Sobrescribir schema para Chile
schema: chile_dbo

# Fechas específicas para Chile
fecha_inicio: "2024-01-01"
fecha_fin: "2024-06-30"

# Configuración adicional
timezone: America/Santiago
```

**config/AR.yaml** (específico para Argentina)
```yaml
codigo_pais: AR
nombre_pais: Argentina

schema: argentina_dbo
fecha_inicio: "2024-03-01"
fecha_fin: "2024-12-31"

timezone: America/Buenos_Aires
```

### Resolución de Parámetros

Los parámetros se resuelven en este orden (el último sobrescribe):

1. `common_params.yaml` → valores base
2. `{PAIS}.yaml` → valores específicos del país

**Ejemplo para Chile:**
```python
# Resultado final de parámetros
{
    "database": "kpi_warehouse",     # de common_params.yaml
    "schema": "chile_dbo",           # sobrescrito por CL.yaml
    "codigo_pais": "CL",             # de CL.yaml
    "fecha_inicio": "2024-01-01",    # sobrescrito por CL.yaml
    "fecha_fin": "2024-06-30",       # sobrescrito por CL.yaml
}
```

---

## Uso del CLI

### Comandos Disponibles

```bash
# Ver todos los comandos
poetry run sql-databricks-bridge --help

# Ver ayuda de un comando específico
poetry run sql-databricks-bridge extract --help
```

### Extraer Datos (SQL Server → Databricks)

```bash
# Extracción básica
poetry run sql-databricks-bridge extract \
  --queries-path ./queries \
  --config-path ./config \
  --country CL \
  --destination /Volumes/mi_catalogo/mi_schema/mi_volume/output

# Extracción con queries específicas
poetry run sql-databricks-bridge extract \
  --queries-path ./queries \
  --config-path ./config \
  --country CL \
  --destination /Volumes/mi_catalogo/mi_schema/mi_volume/output \
  --queries extraer_usuarios,extraer_ventas

# Extracción con sobrescritura
poetry run sql-databricks-bridge extract \
  --queries-path ./queries \
  --config-path ./config \
  --country CL \
  --destination /Volumes/mi_catalogo/mi_schema/mi_volume/output \
  --overwrite
```

### Listar Queries Disponibles

```bash
poetry run sql-databricks-bridge list-queries --queries-path ./queries
```

### Ver Parámetros Resueltos

```bash
# Ver parámetros para Chile
poetry run sql-databricks-bridge show-params \
  --config-path ./config \
  --country CL
```

### Probar Conexiones

```bash
# Probar conexión a SQL Server y Databricks
poetry run sql-databricks-bridge test-connection
```

### Iniciar Servidor API

```bash
# Iniciar en puerto por defecto (8000)
poetry run sql-databricks-bridge serve

# Iniciar en puerto específico
poetry run sql-databricks-bridge serve --port 8080

# Iniciar con hot-reload (desarrollo)
poetry run sql-databricks-bridge serve --reload
```

---

## Uso de la API REST

### Iniciar el Servidor

```bash
poetry run sql-databricks-bridge serve
```

El servidor estará disponible en `http://localhost:8000`

### Documentación Interactiva

- Swagger UI: http://localhost:8000/docs
- ReDoc: http://localhost:8000/redoc

### Endpoints Principales

#### Iniciar Extracción

```bash
curl -X POST http://localhost:8000/extract \
  -H "Content-Type: application/json" \
  -d '{
    "country": "CL",
    "destination": "/Volumes/catalogo/schema/volume/output",
    "queries_path": "/ruta/a/queries",
    "config_path": "/ruta/a/config",
    "queries": ["extraer_usuarios", "extraer_ventas"],
    "chunk_size": 100000,
    "overwrite": false
  }'
```

**Respuesta:**
```json
{
  "job_id": "abc123-def456-...",
  "status": "pending",
  "message": "Extraction job started",
  "queries_count": 2
}
```

#### Consultar Estado de Job

```bash
curl http://localhost:8000/jobs/abc123-def456-...
```

**Respuesta:**
```json
{
  "job_id": "abc123-def456-...",
  "status": "completed",
  "country": "CL",
  "destination": "/Volumes/catalogo/schema/volume/output",
  "queries_total": 2,
  "queries_completed": 2,
  "queries_failed": 0,
  "results": [
    {
      "query_name": "extraer_usuarios",
      "status": "completed",
      "rows_extracted": 15000,
      "duration_seconds": 12.5
    },
    {
      "query_name": "extraer_ventas",
      "status": "completed",
      "rows_extracted": 250000,
      "duration_seconds": 45.2
    }
  ]
}
```

#### Listar Todos los Jobs

```bash
# Todos los jobs
curl http://localhost:8000/jobs

# Filtrar por estado
curl "http://localhost:8000/jobs?status_filter=running"
```

#### Cancelar Job

```bash
curl -X DELETE http://localhost:8000/jobs/abc123-def456-...
```

#### Health Checks

```bash
# Liveness (¿está vivo el servicio?)
curl http://localhost:8000/health/live

# Readiness (¿están las dependencias disponibles?)
curl http://localhost:8000/health/ready
```

---

## Sincronización Databricks → SQL Server

### Crear Tabla de Eventos en Databricks

```sql
CREATE TABLE IF NOT EXISTS bridge.events.bridge_events (
  event_id STRING,
  operation STRING,           -- 'INSERT', 'UPDATE', 'DELETE'
  source_table STRING,        -- Ej: 'catalogo.schema.tabla_origen'
  target_table STRING,        -- Ej: 'dbo.tabla_destino'
  primary_keys ARRAY<STRING>, -- Ej: array('id', 'fecha')
  priority INT DEFAULT 0,
  status STRING DEFAULT 'pending',
  filter_conditions STRING,   -- Opcional: condición WHERE
  rows_expected INT,
  rows_affected INT,
  error_message STRING,
  created_at TIMESTAMP DEFAULT current_timestamp(),
  updated_at TIMESTAMP
);
```

### Insertar Eventos de Sincronización

```sql
-- Ejemplo: Sincronizar nuevos registros
INSERT INTO bridge.events.bridge_events (
  event_id,
  operation,
  source_table,
  target_table,
  primary_keys
) VALUES (
  uuid(),
  'INSERT',
  'mi_catalogo.mi_schema.datos_nuevos',
  'dbo.tabla_destino',
  array('id')
);

-- Ejemplo: Actualizar registros existentes
INSERT INTO bridge.events.bridge_events (
  event_id,
  operation,
  source_table,
  target_table,
  primary_keys,
  filter_conditions
) VALUES (
  uuid(),
  'UPDATE',
  'mi_catalogo.mi_schema.datos_actualizados',
  'dbo.tabla_destino',
  array('id'),
  "fecha_actualizacion >= '2024-01-01'"
);

-- Ejemplo: Eliminar registros
INSERT INTO bridge.events.bridge_events (
  event_id,
  operation,
  source_table,
  target_table,
  primary_keys,
  rows_expected
) VALUES (
  uuid(),
  'DELETE',
  'mi_catalogo.mi_schema.ids_a_eliminar',
  'dbo.tabla_destino',
  array('id'),
  100  -- Límite de seguridad
);
```

### Monitorear Eventos via API

```bash
# Ver eventos pendientes
curl "http://localhost:8000/sync/events?status_filter=pending"

# Ver estado del poller
curl http://localhost:8000/sync/poller/status

# Trigger manual del poller
curl -X POST http://localhost:8000/sync/poller/trigger
```

### WebSocket para Actualizaciones en Tiempo Real

```javascript
// Conectar al WebSocket
const ws = new WebSocket('ws://localhost:8000/sync/events/ws');

ws.onmessage = (event) => {
  const data = JSON.parse(event.data);
  console.log('Evento actualizado:', data);
};

// Suscribirse a eventos específicos
ws.send(JSON.stringify({
  action: 'subscribe',
  event_ids: ['evento-1', 'evento-2']
}));
```

---

## Solución de Problemas

### Error: "DATABRICKS_TOKEN not set"

```bash
# Verificar que la variable está definida
echo $DATABRICKS_TOKEN

# Si está vacía, agregar al .env o exportar
export DATABRICKS_TOKEN="dapi..."
```

### Error: "Connection refused" en SQL Server

1. Verificar que el host es accesible
2. Verificar que el puerto 1433 está abierto
3. Verificar credenciales

```bash
# Probar conectividad
telnet tu-servidor.database.windows.net 1433
```

### Error: "Invalid endpoint id" en Databricks

Esto significa que falta el `DATABRICKS_WAREHOUSE_ID`:

```bash
# Obtener el warehouse ID desde Databricks UI:
# SQL Warehouses → Tu Warehouse → Connection Details → HTTP Path
# El ID es la última parte: /sql/1.0/warehouses/ABC123

export DATABRICKS_WAREHOUSE_ID="ABC123"
```

### Error: "Query not found"

Verificar que el archivo SQL existe:

```bash
# Listar queries disponibles
poetry run sql-databricks-bridge list-queries --queries-path ./queries
```

### Error: "Missing parameters"

Verificar que todos los parámetros están definidos:

```bash
# Ver parámetros resueltos
poetry run sql-databricks-bridge show-params --config-path ./config --country CL
```

### Logs de Depuración

```bash
# Aumentar nivel de log
export LOG_LEVEL=DEBUG
poetry run sql-databricks-bridge extract ...
```

---

## Contacto y Soporte

Para reportar problemas o solicitar nuevas funcionalidades, crear un issue en el repositorio de GitHub.

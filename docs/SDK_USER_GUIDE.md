# Guía de Usuario - SQL-Databricks Bridge SDK

Esta guía describe cómo utilizar el SDK de Python para interactuar con el servicio SQL-Databricks Bridge.

## Tabla de Contenidos

1. [Introducción](#introducción)
2. [Instalación](#instalación)
3. [Quick Start](#quick-start)
4. [Uso Local con API REST](#uso-local-con-api-rest)
5. [Uso en Databricks](#uso-en-databricks)
6. [Operaciones Soportadas](#operaciones-soportadas)
7. [Manejo de Errores](#manejo-de-errores)
8. [Mejores Prácticas](#mejores-prácticas)
9. [Troubleshooting](#troubleshooting)

---

## Introducción

El SDK proporciona dos clientes para diferentes casos de uso:

| Cliente | Caso de Uso | Ambiente |
|---------|-------------|----------|
| `BridgeAPIClient` | Interactuar con el servicio via REST API | Local / Cualquier ambiente con acceso a la API |
| `BridgeEventsClient` | Interactuar directamente con la tabla de eventos | Databricks Jobs / Notebooks |

### Arquitectura

```
┌─────────────────────────────────────────────────────────────────┐
│                        Desarrollador                             │
│  ┌──────────────────┐           ┌──────────────────────────┐    │
│  │  Ambiente Local  │           │  Databricks Job/Notebook │    │
│  │                  │           │                          │    │
│  │ BridgeAPIClient  │           │   BridgeEventsClient     │    │
│  └────────┬─────────┘           └────────────┬─────────────┘    │
└───────────┼──────────────────────────────────┼──────────────────┘
            │                                  │
            ▼                                  ▼
    ┌───────────────┐               ┌───────────────────────┐
    │   REST API    │               │   bridge_events       │
    │   (Bridge     │               │   (Unity Catalog)     │
    │    Service)   │               │                       │
    └───────┬───────┘               └───────────┬───────────┘
            │                                   │
            └───────────────┬───────────────────┘
                            ▼
                   ┌─────────────────┐
                   │  Bridge Worker  │
                   │  (Procesador)   │
                   └────────┬────────┘
                            │
                            ▼
                   ┌─────────────────┐
                   │   SQL Server    │
                   │   (Destino)     │
                   └─────────────────┘
```

---

## Instalación

### Desde Azure DevOps Artifacts

```bash
# Instalación básica
pip install sql-databricks-bridge-sdk \
    --index-url https://pkgs.dev.azure.com/kantarware/_packaging/kwp-pypi/pypi/simple/

# Con soporte para Databricks (incluye PySpark)
pip install sql-databricks-bridge-sdk[databricks] \
    --index-url https://pkgs.dev.azure.com/kantarware/_packaging/kwp-pypi/pypi/simple/
```

### En Databricks Notebook

```python
%pip install sql-databricks-bridge-sdk \
    --index-url https://pkgs.dev.azure.com/kantarware/_packaging/kwp-pypi/pypi/simple/
```

---

## Quick Start

### Escenario 1: Sincronizar datos desde un Job de Databricks

```python
from sql_databricks_bridge_sdk import BridgeEventsClient, SyncEventStatus

# 1. Inicializar el cliente
client = BridgeEventsClient()

# 2. Preparar los datos (ejemplo)
df = spark.sql("""
    SELECT username, region, access_level
    FROM my_catalog.my_schema.processed_data
    WHERE last_updated > current_date() - interval 1 day
""")
row_count = df.count()

# 3. Guardar a tabla staging
df.write.mode("overwrite").saveAsTable(
    "000-sql-databricks-bridge.staging.market_scope_sync"
)

# 4. Crear evento de sincronización
event_id = client.create_insert_event(
    source_table="000-sql-databricks-bridge.staging.market_scope_sync",
    target_table="dbo.MarketScopeAccess",
    primary_keys=["username", "region"],
    rows_expected=row_count
)

print(f"Evento creado: {event_id}")

# 5. Esperar resultado
result = client.wait_for_completion(
    event_id,
    timeout_seconds=600,
    poll_interval=10,
    verbose=True
)

# 6. Verificar resultado
if result.success:
    print(f"✅ Sincronización exitosa: {result.rows_affected} filas")
    if result.warning:
        print(f"⚠️ Advertencia: {result.warning}")
else:
    print(f"❌ Error: {result.error}")
    raise Exception(f"Sync failed: {result.error}")
```

### Escenario 2: Monitorear eventos desde ambiente local

```python
from sql_databricks_bridge_sdk import BridgeAPIClient

# 1. Conectar al servicio
client = BridgeAPIClient(
    base_url="http://bridge-service:8000",
    token="your-api-token"
)

# 2. Verificar conexión
if not client.test_connection():
    raise Exception("No se puede conectar al servicio")

# 3. Listar eventos pendientes
pending = client.sync.list_events(status="pending", limit=10)
print(f"Eventos pendientes: {len(pending)}")

for event in pending:
    print(f"  - {event.event_id}: {event.operation.value} -> {event.target_table}")

# 4. Ver estadísticas
stats = client.sync.get_stats()
print(f"\nEstadísticas totales:")
print(f"  Total: {stats['total']}")
print(f"  Completados: {stats['by_status'].get('completed', 0)}")
print(f"  Fallidos: {stats['by_status'].get('failed', 0)}")
```

---

## Uso Local con API REST

### Configuración del Cliente

```python
from sql_databricks_bridge_sdk import BridgeAPIClient

# Configuración básica
client = BridgeAPIClient(base_url="http://localhost:8000")

# Con autenticación
client = BridgeAPIClient(
    base_url="https://bridge.empresa.com",
    token="eyJ...",
    timeout=60,
    verify_ssl=True
)
```

### Crear Eventos

```python
# INSERT - Insertar nuevos registros
event = client.sync.create_insert_event(
    source_table="catalog.schema.source",
    target_table="dbo.Target",
    primary_keys=["id"],
    rows_expected=1000,
    metadata={"batch_id": "batch-001"}
)

# UPDATE - Actualizar registros existentes
event = client.sync.create_update_event(
    source_table="catalog.schema.updates",
    target_table="dbo.Target",
    primary_keys=["id"],  # Requerido para UPDATE
    rows_expected=50
)

# DELETE - Eliminar registros
event = client.sync.create_delete_event(
    target_table="dbo.Target",
    primary_keys=["id"],  # Requerido para DELETE
    filter_conditions={"status": "inactive"}  # Requerido para DELETE
)
```

### Monitorear Eventos

```python
# Obtener un evento específico
event = client.sync.get_event("insert-abc123")
print(f"Estado: {event.status.value}")

# Listar con filtros
events = client.sync.list_events(
    status="completed",
    operation="INSERT",
    limit=50
)

# Esperar a que complete
result = client.sync.wait_for_completion(
    "insert-abc123",
    timeout_seconds=300,
    poll_interval=5
)
```

### Reintentar y Cancelar

```python
# Reintentar evento fallido
event = client.sync.retry_event("insert-failed123")

# Cancelar evento pendiente
client.sync.cancel_event("insert-pending456")
```

---

## Uso en Databricks

### Configuración del Cliente

```python
from sql_databricks_bridge_sdk import BridgeEventsClient

# Configuración por defecto (usa catálogo estándar)
client = BridgeEventsClient()

# Configuración personalizada
client = BridgeEventsClient(
    catalog="mi-catalogo",
    schema="mi-schema",
    table="mis-eventos"
)
```

### Flujo Típico de un Job

```python
# Notebook: sync_market_data.py

from sql_databricks_bridge_sdk import BridgeEventsClient

def sync_market_data():
    """Sincroniza datos de mercado a SQL Server."""

    client = BridgeEventsClient()

    # Paso 1: Procesar datos
    df = spark.sql("""
        SELECT * FROM raw.market_data
        WHERE process_date = current_date()
    """)

    # Paso 2: Validar datos
    row_count = df.count()
    if row_count == 0:
        print("No hay datos para sincronizar")
        return

    # Paso 3: Guardar a staging
    staging_table = "000-sql-databricks-bridge.staging.market_data"
    df.write.mode("overwrite").saveAsTable(staging_table)

    # Paso 4: Crear evento
    event_id = client.create_insert_event(
        source_table=staging_table,
        target_table="dbo.MarketData",
        primary_keys=["market_id", "date"],
        rows_expected=row_count,
        metadata={
            "job_id": dbutils.notebook.entry_point.getDbutils().notebook().getContext().jobId().get(),
            "run_id": dbutils.notebook.entry_point.getDbutils().notebook().getContext().runId().get()
        }
    )

    # Paso 5: Esperar resultado
    result = client.wait_for_completion(event_id, timeout_seconds=900, verbose=True)

    # Paso 6: Validar resultado
    if not result.success:
        dbutils.notebook.exit(f"FAILED: {result.error}")

    if result.event.discrepancy and result.event.discrepancy > 0:
        print(f"Warning: {result.event.discrepancy} rows with discrepancy")

    print(f"SUCCESS: {result.rows_affected} rows synced in {result.duration_seconds:.1f}s")
    dbutils.notebook.exit(f"SUCCESS: {result.rows_affected}")

# Ejecutar
sync_market_data()
```

### Monitoreo y Limpieza

```python
# Ver estadísticas
stats = client.get_stats()
print(f"Total eventos: {stats['total']}")

# Listar eventos pendientes
pending = client.list_pending_events(limit=20)
for e in pending:
    print(f"{e.event_id}: {e.source_table} -> {e.target_table}")

# Limpiar eventos antiguos (mayores a 7 días)
deleted = client.cleanup_completed_events(days_old=7)
print(f"Eventos eliminados: {deleted}")
```

---

## Operaciones Soportadas

### INSERT

Inserta nuevos registros desde una tabla en Databricks a SQL Server.

```python
event_id = client.create_insert_event(
    source_table="catalog.schema.new_data",
    target_table="dbo.TargetTable",
    primary_keys=["id"],           # Opcional: para detección de duplicados
    rows_expected=1000,            # Opcional: para validación
    metadata={"batch": "001"},     # Opcional: metadatos adicionales
    priority=5                     # Opcional: prioridad (1-10, menor = mayor)
)
```

### UPDATE

Actualiza registros existentes basándose en primary keys.

```python
event_id = client.create_update_event(
    source_table="catalog.schema.updated_data",
    target_table="dbo.TargetTable",
    primary_keys=["id", "region"],  # Requerido
    rows_expected=50,
    priority=3
)
```

### DELETE

Elimina registros basándose en condiciones de filtro.

```python
event_id = client.create_delete_event(
    target_table="dbo.TargetTable",
    primary_keys=["id"],                    # Requerido
    filter_conditions={                      # Requerido
        "status": "inactive",
        "last_updated_before": "2024-01-01"
    },
    priority=7
)
```

---

## Manejo de Errores

### Excepciones Disponibles

```python
from sql_databricks_bridge_sdk import (
    BridgeSDKError,       # Base - todas las excepciones heredan de esta
    ConnectionError,      # Error de conexión
    AuthenticationError,  # Error de autenticación
    EventNotFoundError,   # Evento no encontrado
    TimeoutError,         # Timeout esperando evento
    ValidationError,      # Error de validación de parámetros
)
```

### Ejemplo de Manejo de Errores

```python
from sql_databricks_bridge_sdk import (
    BridgeEventsClient,
    TimeoutError,
    EventNotFoundError,
    ValidationError,
    BridgeSDKError
)

client = BridgeEventsClient()

try:
    # Crear evento
    event_id = client.create_insert_event(
        source_table="catalog.schema.data",
        target_table="dbo.Target",
        primary_keys=["id"]
    )

    # Esperar resultado
    result = client.wait_for_completion(event_id, timeout_seconds=300)

    if result.success:
        print(f"Éxito: {result.rows_affected} filas")
    else:
        # El evento completó pero con error
        print(f"Fallo: {result.error}")

except ValidationError as e:
    print(f"Error de validación: {e.message}")
    # Ej: "primary_keys required for UPDATE operation"

except TimeoutError as e:
    print(f"Timeout: evento {e.event_id} no completó en {e.timeout_seconds}s")
    # Verificar estado del evento
    event = client.get_event(e.event_id)
    print(f"Estado actual: {event.status.value}")

except EventNotFoundError as e:
    print(f"Evento no encontrado: {e.event_id}")

except BridgeSDKError as e:
    # Captura cualquier otro error del SDK
    print(f"Error SDK: {e.message}")
    if e.details:
        print(f"Detalles: {e.details}")
```

---

## Mejores Prácticas

### 1. Validar datos antes de crear eventos

```python
row_count = df.count()
if row_count == 0:
    print("No hay datos - saltando sincronización")
    return

# Incluir rows_expected para validación automática
event_id = client.create_insert_event(
    ...,
    rows_expected=row_count
)
```

### 2. Usar metadata para trazabilidad

```python
event_id = client.create_insert_event(
    ...,
    metadata={
        "job_id": job_id,
        "run_id": run_id,
        "source_query": "SELECT * FROM raw WHERE date = current_date()",
        "triggered_by": "scheduled_job"
    }
)
```

### 3. Configurar timeouts apropiados

```python
# Para volúmenes pequeños (< 10K filas)
result = client.wait_for_completion(event_id, timeout_seconds=120)

# Para volúmenes medianos (10K - 100K filas)
result = client.wait_for_completion(event_id, timeout_seconds=600)

# Para volúmenes grandes (> 100K filas)
result = client.wait_for_completion(event_id, timeout_seconds=1800)
```

### 4. Manejar discrepancias

```python
result = client.wait_for_completion(event_id)

if result.success:
    if result.event.discrepancy and result.event.discrepancy > 0:
        # Algunas filas no se sincronizaron
        print(f"Warning: {result.event.discrepancy} filas con discrepancia")
        # Considerar logging adicional o alertas
```

### 5. Limpiar eventos antiguos periódicamente

```python
# En un job de mantenimiento
deleted = client.cleanup_completed_events(days_old=30)
print(f"Limpiados {deleted} eventos antiguos")
```

---

## Troubleshooting

### El evento queda en estado "pending" por mucho tiempo

1. Verificar que el Bridge Worker esté funcionando
2. Verificar que haya conectividad entre Databricks y SQL Server
3. Revisar logs del Bridge Worker

```python
# Verificar estado del evento
event = client.get_event(event_id)
print(f"Estado: {event.status.value}")
print(f"Creado: {event.created_at}")
```

### Error "PySpark not available"

Este error ocurre cuando se usa `BridgeEventsClient` fuera de Databricks:

```python
# Solución: Usar BridgeAPIClient para ambientes locales
from sql_databricks_bridge_sdk import BridgeAPIClient

client = BridgeAPIClient(base_url="http://bridge-service:8000")
```

### Error "primary_keys required"

Las operaciones UPDATE y DELETE requieren primary_keys:

```python
# Correcto
event_id = client.create_update_event(
    source_table="source",
    target_table="target",
    primary_keys=["id"]  # Requerido!
)
```

### Timeout al esperar evento

```python
try:
    result = client.wait_for_completion(event_id, timeout_seconds=300)
except TimeoutError as e:
    # Obtener estado actual
    event = client.get_event(event_id)

    if event.status.value == "processing":
        # Todavía procesando - puede ser normal para datos grandes
        print("Evento aún procesando, extender timeout")
    elif event.status.value == "blocked":
        # Evento bloqueado - verificar dependencias
        print(f"Evento bloqueado: {event.error_message}")
```

---

## Soporte

Para reportar problemas o solicitar mejoras:

1. Crear issue en el repositorio de Azure DevOps
2. Contactar al equipo de IT Latam

---

*Última actualización: Enero 2024*

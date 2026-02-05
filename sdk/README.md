# SQL-Databricks Bridge SDK

SDK de Python para interactuar con el servicio SQL-Databricks Bridge. Este SDK proporciona dos clientes principales para diferentes casos de uso:

- **BridgeAPIClient**: Para desarrollo local e interacción con el servicio bridge via REST API
- **BridgeEventsClient**: Para jobs/notebooks de Databricks que interactúan directamente con la tabla de eventos

## Instalación

### Desde Azure DevOps Artifacts

```bash
pip install sql-databricks-bridge-sdk --index-url https://pkgs.dev.azure.com/kantarware/_packaging/kwp-pypi/pypi/simple/
```

### Para ambientes Databricks (incluye soporte PySpark)

```bash
pip install sql-databricks-bridge-sdk[databricks]
```

### Para desarrollo

```bash
pip install sql-databricks-bridge-sdk[dev]
```

## Inicio Rápido

### Usando el Cliente REST API (Desarrollo Local)

```python
from sql_databricks_bridge_sdk import BridgeAPIClient

# Inicializar cliente
client = BridgeAPIClient(
    base_url="http://localhost:8000",
    token="tu-token-api"  # Opcional
)

# Verificar conexión
if client.test_connection():
    print("¡Conectado al servicio bridge!")

# Crear un evento INSERT
event = client.sync.create_insert_event(
    source_table="mi-catalogo.mi-schema.datos_origen",
    target_table="dbo.TablaDestino",
    primary_keys=["id"],
    rows_expected=100
)

print(f"Evento creado: {event.event_id}")

# Esperar a que complete
result = client.sync.wait_for_completion(event.event_id, timeout_seconds=300)

if result.success:
    print(f"Sincronizadas exitosamente {result.rows_affected} filas")
else:
    print(f"Sincronización fallida: {result.error}")
```

### Usando el Cliente de Eventos (Jobs/Notebooks de Databricks)

```python
from sql_databricks_bridge_sdk import BridgeEventsClient

# Inicializar cliente (auto-detecta SparkSession)
client = BridgeEventsClient()

# Crear un evento INSERT
event_id = client.create_insert_event(
    source_table="mi-catalogo.mi-schema.datos_procesados",
    target_table="dbo.MarketScopeData",
    primary_keys=["username", "region"],
    rows_expected=500
)

print(f"Evento creado: {event_id}")

# Esperar a que el worker del bridge procese el evento
result = client.wait_for_completion(
    event_id,
    timeout_seconds=600,
    poll_interval=10,
    verbose=True
)

if result.success:
    print(f"Sincronización completada: {result.rows_affected} filas")
else:
    print(f"Sincronización fallida: {result.error}")
```

## Referencia de la API

### BridgeAPIClient

Cliente para interactuar con el servicio bridge via REST API.

```python
client = BridgeAPIClient(
    base_url="http://localhost:8000",  # URL del servicio bridge
    token=None,                         # Token de autenticación API
    timeout=30,                         # Timeout de solicitudes en segundos
    verify_ssl=True                     # Verificar certificados SSL
)
```

#### Operaciones de Sincronización

| Método | Descripción |
|--------|-------------|
| `sync.create_insert_event(...)` | Crear evento INSERT |
| `sync.create_update_event(...)` | Crear evento UPDATE |
| `sync.create_delete_event(...)` | Crear evento DELETE |
| `sync.get_event(event_id)` | Obtener evento por ID |
| `sync.list_events(...)` | Listar eventos con filtros |
| `sync.get_stats()` | Obtener estadísticas de eventos |
| `sync.wait_for_completion(event_id, ...)` | Esperar a que el evento complete |
| `sync.retry_event(event_id)` | Reintentar un evento fallido |
| `sync.cancel_event(event_id)` | Cancelar un evento pendiente |

#### Operaciones de Salud

| Método | Descripción |
|--------|-------------|
| `health.liveness()` | Verificar si el servicio está vivo |
| `health.readiness()` | Obtener estado de salud detallado |
| `health.is_ready()` | Verificar si el servicio está completamente listo |

### BridgeEventsClient

Cliente para interacción directa con la tabla bridge_events en Databricks.

```python
client = BridgeEventsClient(
    catalog="000-sql-databricks-bridge",  # Nombre del catálogo Unity
    schema="events",                       # Nombre del schema
    table="bridge_events",                 # Nombre de la tabla de eventos
    spark=None                             # SparkSession (auto-detectado)
)
```

#### Operaciones de Eventos

| Método | Descripción |
|--------|-------------|
| `create_insert_event(...)` | Crear evento INSERT |
| `create_update_event(...)` | Crear evento UPDATE |
| `create_delete_event(...)` | Crear evento DELETE |
| `get_event(event_id)` | Obtener evento por ID |
| `get_status(event_id)` | Obtener estado del evento (alias) |
| `list_events(...)` | Listar eventos con filtros |
| `list_pending_events(...)` | Listar eventos pendientes |
| `wait_for_completion(event_id, ...)` | Esperar a que complete |
| `get_stats()` | Obtener estadísticas de eventos |
| `delete_event(event_id)` | Eliminar un evento |
| `cleanup_completed_events(days_old)` | Limpiar eventos antiguos |

## Modelos de Datos

### SyncEvent

```python
@dataclass
class SyncEvent:
    event_id: str
    operation: SyncOperation
    source_table: str
    target_table: str
    primary_keys: list[str]
    priority: int
    status: SyncEventStatus
    rows_expected: int | None
    rows_affected: int | None
    discrepancy: int | None
    warning: str | None
    error_message: str | None
    filter_conditions: dict | None
    metadata: dict | None
    created_at: datetime | None
    processed_at: datetime | None
```

### SyncOperation

```python
class SyncOperation(str, Enum):
    INSERT = "INSERT"
    UPDATE = "UPDATE"
    DELETE = "DELETE"
```

### SyncEventStatus

```python
class SyncEventStatus(str, Enum):
    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"
    BLOCKED = "blocked"
```

### EventResult

Retornado por `wait_for_completion()`:

```python
@dataclass
class EventResult:
    event: SyncEvent
    success: bool
    rows_affected: int
    error: str | None
    warning: str | None
    duration_seconds: float
```

## Manejo de Excepciones

```python
from sql_databricks_bridge_sdk import (
    BridgeSDKError,       # Excepción base
    ConnectionError,      # Fallos de conexión
    AuthenticationError,  # Fallos de autenticación
    EventNotFoundError,   # Evento no encontrado
    TimeoutError,         # Timeout de operación
    ValidationError,      # Parámetros inválidos
)

try:
    result = client.sync.wait_for_completion("event-123", timeout_seconds=60)
except TimeoutError as e:
    print(f"Evento {e.event_id} expiró después de {e.timeout_seconds}s")
except EventNotFoundError as e:
    print(f"Evento {e.event_id} no encontrado")
except BridgeSDKError as e:
    print(f"Error SDK: {e.message}")
```

## Ejemplos

### INSERT por Lotes con Validación

```python
from sql_databricks_bridge_sdk import BridgeEventsClient

client = BridgeEventsClient()

# Contar filas en tabla origen
row_count = spark.sql("SELECT COUNT(*) FROM mi_catalogo.schema.datos").collect()[0][0]

# Crear evento con conteo esperado
event_id = client.create_insert_event(
    source_table="mi_catalogo.schema.datos",
    target_table="dbo.DatosDestino",
    primary_keys=["id"],
    rows_expected=row_count,
    metadata={"job_id": dbutils.notebook.getContext().jobId().get()}
)

result = client.wait_for_completion(event_id, timeout_seconds=600)

if result.success:
    if result.event.discrepancy:
        print(f"Advertencia: {result.event.discrepancy} filas con discrepancia")
    else:
        print(f"Todas las {result.rows_affected} filas sincronizadas exitosamente")
```

### UPDATE con Primary Keys

```python
event_id = client.create_update_event(
    source_table="catalogo.schema.registros_actualizados",
    target_table="dbo.MarketScope",
    primary_keys=["username", "region"],
    rows_expected=150,
    priority=3  # Mayor prioridad (1-10, menor = mayor prioridad)
)

result = client.wait_for_completion(event_id)
print(f"Actualizados {result.rows_affected} registros")
```

### DELETE con Condiciones de Filtro

```python
event_id = client.create_delete_event(
    target_table="dbo.RegistrosAntiguos",
    primary_keys=["id"],
    filter_conditions={
        "status": "inactivo",
        "ultima_actualizacion_antes": "2024-01-01"
    }
)

result = client.wait_for_completion(event_id)
print(f"Eliminados {result.rows_affected} registros")
```

### Monitoreo de Estado de Eventos

```python
# Obtener detalles del evento
event = client.get_event("insert-abc123")
print(f"Estado: {event.status.value}")
print(f"Filas afectadas: {event.rows_affected}")

# Listar eventos recientes
events = client.list_events(limit=10)
for e in events:
    print(f"{e.event_id}: {e.status.value}")

# Obtener estadísticas
stats = client.get_stats()
print(f"Total de eventos: {stats['total']}")
print(f"Pendientes: {stats['by_status'].get('pending', 0)}")
print(f"Completados: {stats['by_status'].get('completed', 0)}")
```

## Desarrollo

### Ejecutar Tests

```bash
cd sdk
pip install -e .[dev]
pytest --cov=sql_databricks_bridge_sdk --cov-report=html
```

### Construir el Paquete

```bash
pip install build
python -m build
```

## Licencia

Licencia MIT - Kantar

# Guía de Sincronización Databricks → SQL Server

Esta guía explica cómo los desarrolladores pueden sincronizar datos desde Databricks hacia SQL Server utilizando la tabla de eventos `bridge_events`.

## Arquitectura

```
┌─────────────────────┐     ┌──────────────────────┐     ┌─────────────────┐
│  Databricks Job     │     │  sql-databricks-     │     │   SQL Server    │
│  (Tu código)        │     │  bridge (servicio)   │     │                 │
├─────────────────────┤     ├──────────────────────┤     ├─────────────────┤
│ 1. Preparar datos   │     │                      │     │                 │
│ 2. Insertar evento  │────>│ 3. Polling eventos   │     │                 │
│    en bridge_events │     │ 4. Leer datos fuente │     │                 │
│                     │     │ 5. Ejecutar operación│────>│ 6. INSERT/      │
│                     │     │ 6. Actualizar estado │     │    UPDATE/DELETE│
└─────────────────────┘     └──────────────────────┘     └─────────────────┘
```

## Configuración

### Tabla de Eventos

Los eventos se registran en:
```
`000-sql-databricks-bridge`.`events`.bridge_events
```

### Tabla de Pruebas

Para pruebas existe:
```
`000-sql-databricks-bridge`.`events`.test_market_scope_data
```

## Esquema de `bridge_events`

| Columna | Tipo | Descripción |
|---------|------|-------------|
| `event_id` | STRING | ID único del evento (requerido) |
| `operation` | STRING | `INSERT`, `UPDATE`, `DELETE` (requerido) |
| `source_table` | STRING | Tabla fuente en Databricks (catalog.schema.table) |
| `target_table` | STRING | Tabla destino en SQL Server (schema.table) |
| `primary_keys` | ARRAY<STRING> | Columnas clave para UPDATE/DELETE |
| `priority` | INT | Prioridad del evento (menor = más prioritario) |
| `status` | STRING | `pending`, `processing`, `completed`, `failed` |
| `rows_expected` | INT | Filas esperadas (para validación) |
| `rows_affected` | INT | Filas realmente afectadas (lo llena el servicio) |
| `discrepancy` | INT | Diferencia entre expected y affected |
| `warning` | STRING | Advertencias durante procesamiento |
| `error_message` | STRING | Mensaje de error si falló |
| `filter_conditions` | STRING | JSON con condiciones WHERE (para DELETE) |
| `metadata` | STRING | JSON con metadata adicional |
| `created_at` | TIMESTAMP | Cuándo se creó el evento |
| `processed_at` | TIMESTAMP | Cuándo se procesó |

## Código Python para Databricks Jobs

### Funciones Utilitarias

```python
from pyspark.sql import SparkSession
from datetime import datetime
import uuid
import json
import time

spark = SparkSession.builder.getOrCreate()

# Configuración
BRIDGE_CATALOG = "000-sql-databricks-bridge"
BRIDGE_SCHEMA = "events"
EVENT_TABLE = f"`{BRIDGE_CATALOG}`.`{BRIDGE_SCHEMA}`.bridge_events"
```

### 1. INSERT - Insertar nuevos registros

```python
def sync_insert_to_sql(
    source_table: str,           # Tabla fuente en Databricks (catalog.schema.table)
    target_table: str,           # Tabla destino en SQL Server (schema.table)
    primary_keys: list,          # Columnas clave primaria
    rows_expected: int = None,   # Filas esperadas (opcional)
    priority: int = 5            # Prioridad (1-10, menor = más prioritario)
) -> str:
    """
    Crear evento para insertar datos desde Databricks a SQL Server.

    Args:
        source_table: Tabla fuente en formato 'catalog.schema.table'
        target_table: Tabla destino en SQL Server 'schema.table'
        primary_keys: Lista de columnas clave primaria
        rows_expected: Número esperado de filas (para validación)
        priority: Prioridad del evento (1-10)

    Returns:
        event_id: ID del evento creado
    """
    event_id = f"insert-{uuid.uuid4().hex[:12]}"
    pk_array = ", ".join(f"'{pk}'" for pk in primary_keys)

    spark.sql(f"""
        INSERT INTO {EVENT_TABLE}
        (event_id, operation, source_table, target_table, primary_keys,
         priority, status, rows_expected, created_at)
        VALUES (
            '{event_id}',
            'INSERT',
            '{source_table}',
            '{target_table}',
            ARRAY({pk_array}),
            {priority},
            'pending',
            {rows_expected if rows_expected else 'NULL'},
            current_timestamp()
        )
    """)

    print(f"[INSERT] Evento creado: {event_id}")
    return event_id


# Ejemplo de uso:
# -------------------------------------------------------------

# 1. Preparar datos en una tabla de Databricks
spark.sql("""
    CREATE OR REPLACE TABLE `mi-catalog`.`mi-schema`.datos_nuevos AS
    SELECT username, is_user, is_admin
    FROM mi_tabla_origen
    WHERE fecha_proceso = current_date()
""")

# 2. Verificar cantidad de registros
count = spark.sql("SELECT COUNT(*) FROM `mi-catalog`.`mi-schema`.datos_nuevos").collect()[0][0]
print(f"Registros a sincronizar: {count}")

# 3. Crear evento de sincronización
event_id = sync_insert_to_sql(
    source_table="mi-catalog.mi-schema.datos_nuevos",
    target_table="dbo.MarketScopeAccess",
    primary_keys=["username"],
    rows_expected=count
)
```

### 2. UPDATE - Actualizar registros existentes

```python
def sync_update_to_sql(
    source_table: str,
    target_table: str,
    primary_keys: list,
    rows_expected: int = None,
    priority: int = 5
) -> str:
    """
    Crear evento para actualizar datos en SQL Server.

    La tabla fuente debe contener:
    - Las columnas de primary_keys (para identificar registros)
    - Las columnas a actualizar (con los nuevos valores)
    """
    event_id = f"update-{uuid.uuid4().hex[:12]}"
    pk_array = ", ".join(f"'{pk}'" for pk in primary_keys)

    spark.sql(f"""
        INSERT INTO {EVENT_TABLE}
        (event_id, operation, source_table, target_table, primary_keys,
         priority, status, rows_expected, created_at)
        VALUES (
            '{event_id}',
            'UPDATE',
            '{source_table}',
            '{target_table}',
            ARRAY({pk_array}),
            {priority},
            'pending',
            {rows_expected if rows_expected else 'NULL'},
            current_timestamp()
        )
    """)

    print(f"[UPDATE] Evento creado: {event_id}")
    return event_id


# Ejemplo de uso:
# -------------------------------------------------------------

# 1. Preparar datos con los valores actualizados
spark.sql("""
    CREATE OR REPLACE TABLE `mi-catalog`.`mi-schema`.datos_actualizados AS
    SELECT
        username,           -- clave primaria
        is_admin,           -- campo a actualizar
        is_user             -- campo a actualizar
    FROM mi_tabla_origen
    WHERE necesita_actualizacion = true
""")

# 2. Crear evento de actualización
event_id = sync_update_to_sql(
    source_table="mi-catalog.mi-schema.datos_actualizados",
    target_table="dbo.MarketScopeAccess",
    primary_keys=["username"]  # Columnas para identificar el registro
)
```

### 3. DELETE - Eliminar registros

```python
def sync_delete_from_sql(
    target_table: str,
    primary_keys: list,
    filter_conditions: dict,
    priority: int = 5
) -> str:
    """
    Crear evento para eliminar datos de SQL Server.

    Args:
        target_table: Tabla destino en SQL Server
        primary_keys: Columnas clave (deben estar en filter_conditions)
        filter_conditions: Dict con condiciones WHERE {"columna": "valor"}
    """
    event_id = f"delete-{uuid.uuid4().hex[:12]}"
    pk_array = ", ".join(f"'{pk}'" for pk in primary_keys)
    filter_json = json.dumps(filter_conditions).replace("'", "\\'")

    spark.sql(f"""
        INSERT INTO {EVENT_TABLE}
        (event_id, operation, source_table, target_table, primary_keys,
         priority, status, filter_conditions, created_at)
        VALUES (
            '{event_id}',
            'DELETE',
            '',
            '{target_table}',
            ARRAY({pk_array}),
            {priority},
            'pending',
            '{filter_json}',
            current_timestamp()
        )
    """)

    print(f"[DELETE] Evento creado: {event_id}")
    return event_id


# Ejemplo de uso:
# -------------------------------------------------------------

# Eliminar un registro específico
event_id = sync_delete_from_sql(
    target_table="dbo.MarketScopeAccess",
    primary_keys=["username"],
    filter_conditions={"username": "usuario_a_eliminar"}
)

# Eliminar múltiples registros con condición
event_id = sync_delete_from_sql(
    target_table="dbo.MarketScopeAccess",
    primary_keys=["is_admin"],
    filter_conditions={"is_admin": 0, "is_user": 0}  # usuarios inactivos
)
```

### 4. Verificar Estado del Evento

```python
def check_event_status(event_id: str) -> dict:
    """Consultar el estado de un evento."""
    result = spark.sql(f"""
        SELECT
            event_id,
            operation,
            status,
            rows_expected,
            rows_affected,
            discrepancy,
            warning,
            error_message,
            created_at,
            processed_at
        FROM {EVENT_TABLE}
        WHERE event_id = '{event_id}'
    """).collect()

    if result:
        row = result[0]
        return {
            "event_id": row["event_id"],
            "operation": row["operation"],
            "status": row["status"],
            "rows_expected": row["rows_expected"],
            "rows_affected": row["rows_affected"],
            "discrepancy": row["discrepancy"],
            "warning": row["warning"],
            "error_message": row["error_message"],
            "created_at": row["created_at"],
            "processed_at": row["processed_at"]
        }
    return None


# Ejemplo de uso:
status = check_event_status(event_id)
print(f"Estado: {status['status']}")
print(f"Filas afectadas: {status['rows_affected']}")
if status['error_message']:
    print(f"Error: {status['error_message']}")
```

### 5. Esperar a que se Procese el Evento

```python
def wait_for_event(
    event_id: str,
    timeout_seconds: int = 300,
    poll_interval: int = 10
) -> dict:
    """
    Esperar a que un evento se procese.

    Args:
        event_id: ID del evento a monitorear
        timeout_seconds: Tiempo máximo de espera (default: 5 min)
        poll_interval: Intervalo entre verificaciones (default: 10 seg)

    Returns:
        Estado final del evento

    Raises:
        TimeoutError: Si el evento no se procesa en el tiempo límite
    """
    start = time.time()

    while time.time() - start < timeout_seconds:
        status = check_event_status(event_id)

        if status is None:
            raise ValueError(f"Evento no encontrado: {event_id}")

        if status['status'] in ('completed', 'failed'):
            return status

        elapsed = int(time.time() - start)
        print(f"[{elapsed}s] Esperando... estado: {status['status']}")
        time.sleep(poll_interval)

    raise TimeoutError(f"Evento {event_id} no se procesó en {timeout_seconds}s")


# Ejemplo de uso:
try:
    result = wait_for_event(event_id, timeout_seconds=120)

    if result['status'] == 'completed':
        print(f"Sincronización exitosa!")
        print(f"  Filas afectadas: {result['rows_affected']}")
        if result['warning']:
            print(f"  Advertencia: {result['warning']}")
    else:
        print(f"Sincronización falló: {result['error_message']}")

except TimeoutError as e:
    print(f"Timeout: {e}")
```

## Ejemplo Completo: Job de Sincronización

```python
"""
Job de sincronización de datos de precios a SQL Server.
"""

from pyspark.sql import SparkSession
import uuid
import json
import time

spark = SparkSession.builder.getOrCreate()

# Configuración
BRIDGE_CATALOG = "000-sql-databricks-bridge"
EVENT_TABLE = f"`{BRIDGE_CATALOG}`.`events`.bridge_events"

def main():
    # 1. Preparar datos a sincronizar
    print("Preparando datos...")

    spark.sql("""
        CREATE OR REPLACE TABLE `003-precios`.`bronze-data`.sync_temp AS
        SELECT
            username,
            is_user,
            is_admin
        FROM `003-precios`.`bronze-data`.usuarios_actualizados
        WHERE sync_pending = true
    """)

    # 2. Contar registros
    count = spark.sql(
        "SELECT COUNT(*) FROM `003-precios`.`bronze-data`.sync_temp"
    ).collect()[0][0]

    if count == 0:
        print("No hay datos para sincronizar")
        return

    print(f"Registros a sincronizar: {count}")

    # 3. Crear evento de sincronización
    event_id = f"sync-precios-{uuid.uuid4().hex[:8]}"

    spark.sql(f"""
        INSERT INTO {EVENT_TABLE}
        (event_id, operation, source_table, target_table, primary_keys,
         status, rows_expected, metadata, created_at)
        VALUES (
            '{event_id}',
            'INSERT',
            '003-precios.bronze-data.sync_temp',
            'dbo.MarketScopeAccess',
            ARRAY('username'),
            'pending',
            {count},
            '{{"job": "sync_precios", "date": "{time.strftime("%Y-%m-%d")}"}}',
            current_timestamp()
        )
    """)

    print(f"Evento creado: {event_id}")

    # 4. Esperar procesamiento
    print("Esperando procesamiento...")
    start = time.time()
    timeout = 300  # 5 minutos

    while time.time() - start < timeout:
        result = spark.sql(f"""
            SELECT status, rows_affected, error_message
            FROM {EVENT_TABLE}
            WHERE event_id = '{event_id}'
        """).collect()[0]

        if result['status'] == 'completed':
            print(f"Sincronización completada: {result['rows_affected']} filas")

            # Marcar registros como sincronizados
            spark.sql("""
                UPDATE `003-precios`.`bronze-data`.usuarios_actualizados
                SET sync_pending = false
                WHERE sync_pending = true
            """)
            return

        elif result['status'] == 'failed':
            raise Exception(f"Sincronización falló: {result['error_message']}")

        time.sleep(10)

    raise TimeoutError("Timeout esperando sincronización")

if __name__ == "__main__":
    main()
```

## Consideraciones Importantes

### 1. Columnas IDENTITY

SQL Server tiene columnas IDENTITY (auto-incrementales). **No incluir estas columnas** en la tabla fuente para INSERT:

```python
# MAL - incluye 'id' que es IDENTITY
spark.sql("""
    SELECT id, username, is_user FROM ...
""")

# BIEN - excluye 'id', SQL Server lo genera
spark.sql("""
    SELECT username, is_user FROM ...
""")
```

### 2. Nombres de Tablas con Guiones

Usar backticks para catálogos/schemas con guiones:

```python
# Correcto
source_table = "`mi-catalog`.`mi-schema`.mi_tabla"

# Incorrecto - fallará
source_table = "mi-catalog.mi-schema.mi_tabla"
```

### 3. El Servicio Debe Estar Corriendo

El servicio `sql-databricks-bridge` debe estar ejecutándose para procesar eventos. Contactar al equipo de infraestructura si los eventos quedan en `pending` por mucho tiempo.

### 4. Límite de DELETE

Por seguridad, las operaciones DELETE tienen un límite máximo de filas. Si necesitas eliminar más registros, dividir en múltiples eventos.

### 5. Transacciones

Cada evento es una transacción independiente. Si necesitas atomicidad entre múltiples operaciones, considerar crear un evento con toda la data necesaria.

## Troubleshooting

### Evento queda en "pending"

1. Verificar que el servicio bridge está corriendo
2. Revisar logs del servicio
3. Verificar conectividad a SQL Server

### Evento falla con "No data to insert"

1. Verificar que la tabla fuente tiene datos
2. Verificar que el nombre de tabla es correcto (con backticks si tiene guiones)

### Error de IDENTITY

Si ves error "Cannot insert explicit value for identity column":
- Excluir la columna IDENTITY de la tabla fuente
- Crear una vista sin esa columna

### Discrepancia en rows_affected

Si `rows_affected != rows_expected`:
- Para INSERT: posibles duplicados en la fuente
- Para UPDATE: registros no encontrados en destino
- Para DELETE: condiciones no coinciden

## Contacto

Para soporte o reportar problemas, contactar al equipo de Data Engineering.

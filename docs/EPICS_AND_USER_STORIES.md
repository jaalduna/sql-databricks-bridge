# SQL-Databricks Bridge - Épicas e Historias de Usuario

> **Documento de Discovery**
> **Fecha:** 2026-01-23
> **Versión:** 1.1
> **Estado:** Draft para validación
> **Última actualización:** Tabla de eventos y validaciones de seguridad

---

## Visión del Producto

**SQL-Databricks Bridge** es un servicio genérico y estable que actúa como puente bidireccional entre bases de datos SQL Server y Databricks. Permite a múltiples proyectos definir sus propias queries y configuraciones sin modificar el código fuente del servicio.

### Principios de Diseño

1. **Desacoplado:** El servicio no contiene queries específicas de ningún proyecto
2. **Configurable:** Cada proyecto define sus queries, parámetros y destinos
3. **Estable:** Una vez deployado, no requiere modificaciones de código
4. **Multi-tenant:** Múltiples proyectos pueden usarlo simultáneamente sin bloquearse
5. **Seguro:** Autenticación por token y permisos granulares por tabla

---

## Arquitectura de Alto Nivel

```
┌─────────────────────────────────────────────────────────────────────────┐
│                        PROYECTOS USUARIOS                               │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐                     │
│  │ kwp-merge   │  │ precios     │  │ simulador   │  ...                │
│  │ └─queries/  │  │ └─queries/  │  │ └─queries/  │                     │
│  └─────────────┘  └─────────────┘  └─────────────┘                     │
└─────────────────────────────────────────────────────────────────────────┘
            │                │                │
            ▼                ▼                ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                    SQL-DATABRICKS BRIDGE SERVICE                        │
│  ┌──────────────────────────────────────────────────────────────────┐  │
│  │  Interfaces: REST API | CLI | Python SDK                         │  │
│  └──────────────────────────────────────────────────────────────────┘  │
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────────┐    │
│  │ Auth & Permisos │  │ Query Executor  │  │ Event Poller (10s)  │    │
│  └─────────────────┘  └─────────────────┘  └─────────────────────┘    │
└─────────────────────────────────────────────────────────────────────────┘
            │                                        │
            ▼                                        ▼
┌───────────────────────┐              ┌───────────────────────────────┐
│   SQL Servers         │              │   Databricks                  │
│   (múltiples países)  │◄────────────►│   ├── Volumes (datos)         │
│                       │              │   └── Events Table (comandos) │
└───────────────────────┘              └───────────────────────────────┘
```

---

## Épica 1: Flujo SQL → Databricks (Extracción)

> Como usuario de un proyecto, quiero ejecutar mis queries SQL y enviar los resultados a Databricks sin tener que modificar el servicio bridge.

### US-1.1: Definir queries en path externo

**Como** usuario de un proyecto
**Quiero** definir mis queries SQL en una carpeta de mi proyecto
**Para** no depender del código fuente del servicio bridge

**Criterios de aceptación:**
- [ ] Puedo crear archivos `.sql` en cualquier ruta accesible por el servicio
- [ ] El servicio descubre automáticamente todos los archivos `.sql` en el path indicado
- [ ] Los nombres de archivo determinan el nombre del output (ej: `ventas.sql` → `ventas.parquet`)

**Ejemplo de estructura:**
```
mi-proyecto/
└── queries/
    ├── ventas.sql
    ├── productos.sql
    └── clientes.sql
```

---

### US-1.2: Queries sin parámetros

**Como** usuario
**Quiero** poder escribir queries SQL simples sin ninguna parametrización
**Para** casos donde la query es igual para todos los países

**Criterios de aceptación:**
- [ ] Una query sin placeholders `{param}` se ejecuta tal cual
- [ ] No es obligatorio tener archivos de configuración

**Ejemplo:**
```sql
-- queries/productos.sql
SELECT id, nombre, precio FROM dbo.Productos WHERE activo = 1
```

---

### US-1.3: Queries parametrizadas por país

**Como** usuario
**Quiero** escribir una sola query con parámetros que se resuelven según el país
**Para** no tener que duplicar queries por cada país

**Criterios de aceptación:**
- [ ] Puedo usar sintaxis `{nombre_parametro}` en mis queries
- [ ] Defino los valores en archivos YAML por país
- [ ] Si un parámetro no existe para un país, el servicio falla con mensaje claro

**Ejemplo query:**
```sql
-- queries/ventas.sql
SELECT * FROM {tabla_ventas} WHERE Periodo >= {periodo_inicio}
```

**Ejemplo configuración:**
```yaml
# config/Colombia.yaml
tabla_ventas: "LOC_Ventas_CO"
periodo_inicio: 202301

# config/Mexico.yaml
tabla_ventas: "LOC_Ventas_MX"
periodo_inicio: 202301
```

---

### US-1.4: Parámetros comunes entre países

**Como** usuario
**Quiero** definir parámetros que aplican a todos los países
**Para** no repetir configuración

**Criterios de aceptación:**
- [ ] Existe un archivo `common_params.yaml` con valores por defecto
- [ ] Los archivos por país pueden sobrescribir valores comunes
- [ ] Orden de precedencia: país > common

**Ejemplo:**
```yaml
# config/common_params.yaml
periodo_inicio: 202301
columna_factor: "factor_rw1 as factor_rw"

# config/Colombia.yaml (sobrescribe periodo_inicio)
periodo_inicio: 202306
```

---

### US-1.5: Especificar destino en Databricks

**Como** usuario
**Quiero** indicar dónde en Databricks deben guardarse los resultados
**Para** organizar los datos según las necesidades de mi proyecto

**Criterios de aceptación:**
- [ ] Puedo especificar el path de destino en Databricks Volumes
- [ ] El path puede incluir variables como `{country}`, `{date}`
- [ ] Si el archivo existe, puedo elegir sobrescribir o saltar

**Ejemplo de invocación:**
```bash
sql-databricks-bridge extract \
  --queries-path ./queries \
  --config-path ./config \
  --country Colombia \
  --destination /Volumes/catalog/schema/volume/{country}/
```

---

### US-1.6: Interfaz REST API

**Como** sistema automatizado
**Quiero** invocar el servicio via HTTP
**Para** integrarlo en pipelines de CI/CD o aplicaciones

**Criterios de aceptación:**
- [ ] Endpoint POST `/extract` acepta parámetros de extracción
- [ ] Respuesta incluye job_id para tracking
- [ ] Endpoint GET `/jobs/{job_id}` retorna estado

**Ejemplo request:**
```json
POST /extract
Authorization: Bearer <token>
{
  "queries_path": "/shared/proyecto-x/queries",
  "config_path": "/shared/proyecto-x/config",
  "country": "Colombia",
  "destination": "/Volumes/catalog/schema/volume/colombia/"
}
```

---

### US-1.7: Interfaz CLI

**Como** desarrollador
**Quiero** invocar el servicio desde línea de comandos
**Para** ejecutar extracciones manualmente o desde scripts

**Criterios de aceptación:**
- [ ] Comando `sql-databricks-bridge extract` con opciones claras
- [ ] Soporte para `--token` o variable de entorno
- [ ] Output muestra progreso y resultado

---

### US-1.8: SDK Python *(Post-MVP)*

> **Nota:** Esta historia es para una fase posterior al MVP.

**Como** desarrollador Python
**Quiero** importar el bridge como librería
**Para** integrarlo directamente en mi código

**Criterios de aceptación:**
- [ ] `pip install sql-databricks-bridge`
- [ ] API clara y documentada
- [ ] Soporte async opcional

**Ejemplo:**
```python
from sql_databricks_bridge import BridgeClient

client = BridgeClient(token="abc123")
result = client.extract(
    queries_path="./queries",
    config_path="./config",
    country="Colombia",
    destination="/Volumes/catalog/schema/volume/colombia/"
)
print(result.status)  # "completed"
```

---

## Épica 2: Flujo Databricks → SQL (Sincronización Inversa)

> Como usuario, quiero que los resultados de mis jobs en Databricks se sincronicen de vuelta a SQL Server.

### US-2.1: Tabla de eventos en Databricks

**Como** servicio bridge
**Quiero** monitorear una tabla de eventos en Databricks
**Para** detectar solicitudes de sincronización desde los jobs

**Criterios de aceptación:**
- [ ] El servicio hace polling cada 10 segundos
- [ ] Lee eventos con status = 'pending'
- [ ] Procesa eventos en orden de prioridad (mayor primero)
- [ ] Estructura de tabla definida y documentada

**Estructura de tabla de eventos:**
```sql
CREATE TABLE bridge_events (
  -- Identificación (generado por el sistema)
  event_id STRING,                -- UUID generado automáticamente

  -- Operación
  operation STRING,               -- 'INSERT', 'UPDATE', 'DELETE'
  source_table STRING,            -- formato: catalog.schema.tabla (ej: kpi_prd_01.silver.resultados)
  target_table STRING,            -- tabla destino en SQL Server (ej: dbo.Resultados)
  country STRING,
  primary_keys ARRAY<STRING>,     -- columnas PK para UPDATE/DELETE (ej: ['id', 'country'])

  -- Control de ejecución
  priority INT DEFAULT 0,         -- mayor = más prioritario, 0 = normal
  status STRING,                  -- 'pending', 'processing', 'completed', 'failed'
  retry_count INT DEFAULT 0,      -- intentos realizados

  -- Resultados
  rows_expected INT,              -- filas en source_table
  rows_affected INT,              -- filas realmente modificadas en SQL
  discrepancy INT,                -- diferencia (expected - affected)
  warning STRING,                 -- mensaje si hay discrepancia
  error_message STRING,           -- descripción del error si failed
  execution_time_ms BIGINT,       -- duración de la operación

  -- Auditoría
  created_by STRING,              -- token/usuario que creó el evento
  created_at TIMESTAMP,
  updated_at TIMESTAMP
)
```

**Ejemplo de evento INSERT:**
```python
{
  "operation": "INSERT",
  "source_table": "kpi_prd_01.silver.calibracion_output",
  "target_table": "dbo.Calibracion_Resultados",
  "country": "Colombia",
  "primary_keys": [],  # No requerido para INSERT
  "priority": 0
}
```

**Ejemplo de evento DELETE:**
```python
{
  "operation": "DELETE",
  "source_table": "kpi_prd_01.silver.ids_a_eliminar",
  "target_table": "dbo.Ventas_Obsoletas",
  "country": "Colombia",
  "primary_keys": ["id_venta", "id_producto"],  # Obligatorio
  "priority": 5  # Alta prioridad
}
```

---

### US-2.2: Operación INSERT

**Como** usuario desde Databricks
**Quiero** insertar registros nuevos en una tabla SQL Server
**Para** agregar resultados calculados al sistema transaccional

**Criterios de aceptación:**
- [ ] El servicio lee los datos de `source_table` en Databricks
- [ ] Las columnas de `source_table` deben coincidir exactamente con `target_table`
- [ ] Ejecuta INSERT en `target_table` de SQL Server
- [ ] Reporta `rows_affected` en el evento
- [ ] Actualiza status del evento a 'completed' o 'failed'

---

### US-2.3: Operación UPDATE

**Como** usuario desde Databricks
**Quiero** actualizar registros existentes en SQL Server
**Para** modificar datos basados en cálculos de Databricks

**Criterios de aceptación:**
- [ ] `primary_keys` es **obligatorio** (falla si está vacío)
- [ ] Las columnas de `source_table` deben coincidir exactamente con `target_table`
- [ ] El servicio genera UPDATE statements basados en las PKs
- [ ] Reporta `rows_expected` vs `rows_affected` y calcula `discrepancy`
- [ ] Si hay discrepancia, agrega `warning` descriptivo

**Ejemplo de resultado con discrepancia:**
```python
{
  "status": "completed",
  "rows_expected": 1000,
  "rows_affected": 850,
  "discrepancy": 150,
  "warning": "150 rows in source did not match any records in target"
}
```

---

### US-2.4: Operación DELETE

**Como** usuario desde Databricks
**Quiero** eliminar registros de SQL Server
**Para** limpiar datos obsoletos según lógica de negocio

**Criterios de aceptación:**
- [ ] `primary_keys` es **obligatorio** (falla si está vacío)
- [ ] La tabla fuente contiene los IDs a eliminar
- [ ] Validación: verificar que no hay duplicados en PKs de `source_table` (ver US-2.7)
- [ ] Reporta `rows_expected` vs `rows_affected` y calcula `discrepancy`
- [ ] Si hay discrepancia, agrega `warning` descriptivo

---

### US-2.5: Visibilidad del estado del evento

**Como** usuario desde Databricks
**Quiero** ver el estado de mi solicitud
**Para** saber si se completó o falló

**Criterios de aceptación:**
- [ ] El servicio actualiza `status` en la tabla de eventos
- [ ] Si falla, `error_message` contiene descripción útil del error
- [ ] `updated_at` refleja cuándo cambió el estado
- [ ] `rows_expected`, `rows_affected`, `discrepancy` siempre se populan
- [ ] `execution_time_ms` indica la duración
- [ ] `retry_count` indica cuántos intentos se realizaron

**Estados posibles:**
| Status | Descripción |
|--------|-------------|
| pending | Esperando ser procesado |
| processing | El servicio está ejecutando la operación |
| completed | Operación exitosa (puede tener warning si hay discrepancia) |
| failed | Error durante ejecución (ver error_message) |

**Campos de resultado:**
| Campo | Descripción |
|-------|-------------|
| rows_expected | Cantidad de filas en source_table |
| rows_affected | Cantidad de filas modificadas en SQL Server |
| discrepancy | Diferencia: expected - affected |
| warning | Mensaje explicativo si discrepancy > 0 |
| execution_time_ms | Tiempo de ejecución en milisegundos |

---

### US-2.6: Reintentos automáticos

**Como** usuario
**Quiero** que el servicio reintente operaciones fallidas por errores transitorios
**Para** no tener que intervenir manualmente

**Criterios de aceptación:**
- [ ] Máximo 3 reintentos para errores de conexión
- [ ] Backoff exponencial entre reintentos
- [ ] `retry_count` se incrementa en cada intento
- [ ] Después del 3er intento, status = 'failed'

---

### US-2.7: Validación de duplicados en PKs

**Como** servicio bridge
**Quiero** validar que no hay duplicados en las primary keys de source_table
**Para** prevenir comportamientos inesperados en UPDATE/DELETE

**Criterios de aceptación:**
- [ ] Antes de ejecutar UPDATE/DELETE, verificar duplicados en PKs
- [ ] Si hay duplicados, el evento falla con mensaje claro
- [ ] Costo de validación: ~2-5 segundos (acceptable)

**Query de validación:**
```sql
SELECT COUNT(*) as total, COUNT(DISTINCT pk1, pk2, ...) as unique_keys
FROM source_table
-- Si total != unique_keys → hay duplicados
```

**Ejemplo de error:**
```python
{
  "status": "failed",
  "error_message": "Duplicate primary keys found in source_table: 150 duplicates detected for keys ['id_venta', 'id_producto']. Please deduplicate before retrying."
}
```

---

### US-2.8: Warning para operaciones grandes

**Como** usuario
**Quiero** recibir un warning cuando mi operación afecta muchas filas
**Para** estar consciente del impacto antes de que se complete

**Criterios de aceptación:**
- [ ] Si la operación afecta más de 1,000,000 de filas, agregar warning
- [ ] El warning no bloquea la operación, solo informa
- [ ] El warning se registra en el campo `warning` del evento

**Ejemplo de warning:**
```python
{
  "status": "completed",
  "rows_affected": 2500000,
  "warning": "Large operation: 2,500,000 rows affected. Consider batching for future operations."
}
```

---

## Épica 3: Autenticación y Autorización

> Como administrador de TI, quiero controlar quién puede usar el servicio y qué operaciones puede realizar.

### US-3.1: Autenticación por token

**Como** administrador de TI
**Quiero** que los usuarios se autentiquen con un token
**Para** controlar el acceso al servicio

**Criterios de aceptación:**
- [ ] Cada proyecto/usuario tiene un token único
- [ ] Requests sin token válido reciben 401 Unauthorized
- [ ] Tokens se pueden revocar sin afectar otros usuarios

---

### US-3.2: Permisos de lectura por tabla

**Como** administrador de TI
**Quiero** definir qué tablas puede leer cada usuario
**Para** proteger datos sensibles

**Criterios de aceptación:**
- [ ] Configuración define permisos de lectura por token
- [ ] Si un usuario intenta leer tabla no autorizada, recibe 403 Forbidden
- [ ] Mensaje de error indica qué permiso falta

---

### US-3.3: Permisos de escritura por tabla

**Como** administrador de TI
**Quiero** definir qué tablas puede modificar cada usuario
**Para** evitar modificaciones no autorizadas

**Criterios de aceptación:**
- [ ] Permisos separados: `read` vs `read_write`
- [ ] Operaciones INSERT/UPDATE/DELETE requieren `read_write`
- [ ] Un usuario con `read` no puede crear eventos de sincronización inversa para esa tabla

---

### US-3.4: Archivo de configuración de permisos

**Como** administrador de TI
**Quiero** configurar permisos en un archivo YAML
**Para** gestionar accesos de forma simple

**Criterios de aceptación:**
- [ ] Archivo centralizado gestionado por TI
- [ ] Cambios no requieren reiniciar el servicio (hot reload)
- [ ] Formato claro y validado

**Ejemplo:**
```yaml
# permissions.yaml
users:
  - token: "proyecto-precios-token-abc123"
    name: "proyecto-precios"
    permissions:
      - table: "dbo.J_AtosCompra_New"
        access: "read"
      - table: "dbo.Resultados_Precios"
        access: "read_write"
      - table: "dbo.VW_ArtigoZ"
        access: "read"

  - token: "proyecto-simulador-token-xyz789"
    name: "proyecto-simulador"
    permissions:
      - table: "dbo.*"  # wildcard para todas las tablas de dbo
        access: "read"
      - table: "dbo.Simulacion_Output"
        access: "read_write"

  - token: "proyecto-calibracion-token-def456"
    name: "proyecto-calibracion"
    permissions:
      - table: "dbo.Panelistas"
        access: "read_write"
        max_delete_rows: 10000  # límite de seguridad para DELETE
      - table: "dbo.Calibracion_Historico"
        access: "read_write"
        max_delete_rows: 50000
```

---

### US-3.5: Protección de tablas críticas

**Como** administrador de TI
**Quiero** limitar la cantidad de filas que se pueden eliminar de tablas críticas
**Para** prevenir borrados masivos accidentales

**Criterios de aceptación:**
- [ ] Configuración `max_delete_rows` por tabla en permisos
- [ ] Si DELETE afectaría más filas que el límite, la operación falla
- [ ] Mensaje de error claro indica el límite y cuántas filas se intentaron borrar
- [ ] El límite solo aplica a DELETE, no a INSERT/UPDATE

**Ejemplo de error:**
```python
{
  "status": "failed",
  "error_message": "DELETE blocked: operation would affect 75,000 rows but max_delete_rows for dbo.Panelistas is 10,000. Contact admin to increase limit or batch the operation."
}
```

---

### US-3.6: Logs de auditoría

**Como** administrador de TI
**Quiero** ver quién ejecutó qué operaciones
**Para** auditoría y troubleshooting

**Criterios de aceptación:**
- [ ] Cada operación se registra con: timestamp, usuario, operación, tablas, resultado
- [ ] Logs accesibles para administradores
- [ ] Retención configurable

---

## Épica 4: Operación y Monitoreo

> Como operador del servicio, quiero monitorear su salud y rendimiento.

### US-4.1: Health check endpoint

**Como** sistema de monitoreo
**Quiero** un endpoint de health check
**Para** detectar si el servicio está funcionando

**Criterios de aceptación:**
- [ ] GET `/health` retorna 200 si el servicio está operativo
- [ ] Incluye checks de conectividad a SQL y Databricks

---

### US-4.2: Métricas de operación

**Como** operador
**Quiero** ver métricas del servicio
**Para** detectar problemas de rendimiento

**Criterios de aceptación:**
- [ ] Cantidad de extracciones por período
- [ ] Cantidad de sincronizaciones inversas por período
- [ ] Tiempo promedio de ejecución
- [ ] Tasa de errores

---

### US-4.3: Configuración sin reinicio

**Como** operador
**Quiero** actualizar configuración sin reiniciar el servicio
**Para** minimizar downtime

**Criterios de aceptación:**
- [ ] Permisos se recargan automáticamente
- [ ] Intervalo de polling configurable en runtime

---

## Épica 5: Documentación y Onboarding

> Como nuevo usuario del servicio, quiero documentación clara para empezar a usarlo.

### US-5.1: Guía de inicio rápido

**Como** nuevo usuario
**Quiero** una guía paso a paso
**Para** hacer mi primera extracción en minutos

**Criterios de aceptación:**
- [ ] Documento con ejemplo mínimo funcional
- [ ] Incluye estructura de carpetas requerida
- [ ] Comandos copy-paste que funcionan

---

### US-5.2: Referencia de API

**Como** desarrollador
**Quiero** documentación completa de la API
**Para** conocer todos los endpoints y opciones

**Criterios de aceptación:**
- [ ] OpenAPI/Swagger spec
- [ ] Ejemplos de requests y responses
- [ ] Códigos de error documentados

---

### US-5.3: Ejemplos por caso de uso

**Como** usuario
**Quiero** ejemplos de casos comunes
**Para** adaptar a mi proyecto

**Criterios de aceptación:**
- [ ] Ejemplo: extracción simple sin parámetros
- [ ] Ejemplo: extracción parametrizada multi-país
- [ ] Ejemplo: sincronización inversa (INSERT)
- [ ] Ejemplo: sincronización inversa (UPDATE)

---

## Resumen de Épicas

| # | Épica | Historias | Prioridad | Notas |
|---|-------|-----------|-----------|-------|
| 1 | Flujo SQL → Databricks | US-1.1 a US-1.8 | Alta | US-1.8 (SDK) es post-MVP |
| 2 | Flujo Databricks → SQL | US-2.1 a US-2.8 | Alta | Incluye validaciones de seguridad |
| 3 | Autenticación y Autorización | US-3.1 a US-3.6 | Alta | Incluye protección de tablas críticas |
| 4 | Operación y Monitoreo | US-4.1 a US-4.3 | Media | |
| 5 | Documentación y Onboarding | US-5.1 a US-5.3 | Media | |

### Historias para MVP vs Post-MVP

**MVP:**
- Épica 1: US-1.1 a US-1.7 (sin SDK Python)
- Épica 2: US-2.1 a US-2.8 (completa)
- Épica 3: US-3.1 a US-3.6 (completa)
- Épica 4: US-4.1 (health check mínimo)
- Épica 5: US-5.1 (guía de inicio rápido)

**Post-MVP:**
- US-1.8: SDK Python
- US-4.2: Métricas avanzadas
- US-4.3: Hot reload completo
- US-5.2, US-5.3: Documentación completa

---

## Próximos Pasos

1. [ ] Validar épicas e historias con stakeholders
2. [ ] Priorizar historias para MVP
3. [ ] Estimar esfuerzo por historia
4. [ ] Definir arquitectura técnica detallada
5. [ ] Crear backlog en herramienta de gestión

---

## Glosario

| Término | Definición |
|---------|------------|
| **Bridge** | Servicio que conecta SQL Server con Databricks |
| **Extracción** | Flujo SQL → Databricks |
| **Sincronización inversa** | Flujo Databricks → SQL |
| **Evento** | Registro en tabla de Databricks que solicita una operación |
| **Token** | Credencial de autenticación por proyecto |
| **Query parametrizada** | Query SQL con placeholders que se resuelven según configuración |
| **Primary Keys (PKs)** | Columnas que identifican unívocamente un registro para UPDATE/DELETE |
| **Discrepancy** | Diferencia entre filas esperadas y filas realmente afectadas |
| **Polling** | Consulta periódica (cada 10s) a la tabla de eventos |
| **Hot reload** | Actualización de configuración sin reiniciar el servicio |
| **max_delete_rows** | Límite de seguridad de filas que se pueden eliminar de una tabla |

---

## Decisiones de Diseño

| Decisión | Alternativas consideradas | Justificación |
|----------|---------------------------|---------------|
| Polling cada 10s | WebSocket, Push notifications | Databricks no puede ver el servidor del servicio |
| PKs obligatorias para UPDATE/DELETE | Inferir de metadata | Seguridad: prevenir borrados masivos accidentales |
| Validación de duplicados en PKs | Validar existencia en target | Balance costo/beneficio: duplicados es el error más común |
| Reporte de discrepancia | Validación previa de existencia | Menor costo, usuario tiene visibilidad del resultado |
| Permisos en archivo YAML | Base de datos, Azure AD | Simplicidad para fase inicial |

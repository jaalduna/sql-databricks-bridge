# SQL-Databricks Bridge — Referencia de API

Base URL: `/api/v1`

---

## Salud (`/health`)

| Metodo | Ruta | Descripcion |
|--------|------|-------------|
| GET | `/health/live` | Sonda de vida — confirma que el servicio esta corriendo |
| GET | `/health/ready` | Sonda de disponibilidad — verifica dependencias (SQL Server, Databricks) |
| GET | `/health/startup` | Sonda de arranque — confirma que el servicio inicio correctamente |

---

## Autenticacion (`/auth`)

| Metodo | Ruta | Descripcion |
|--------|------|-------------|
| GET | `/auth/me` | Obtener info del usuario actual (identidad, roles, paises autorizados) |

---

## Metadata (`/metadata`)

| Metodo | Ruta | Descripcion |
|--------|------|-------------|
| GET | `/metadata/countries` | Listar paises disponibles y sus queries SQL |
| GET | `/metadata/stages` | Listar etapas del pipeline (desde config YAML) |
| GET | `/metadata/data-availability` | Verificar disponibilidad de datos pesaje/elegibilidad por pais y periodo |

---

## Trigger / Eventos (`/trigger`, `/events`)

| Metodo | Ruta | Descripcion |
|--------|------|-------------|
| POST | `/trigger` | Disparar extraccion de datos (SQL Server -> Databricks) para un pais |
| GET | `/events` | Listar eventos de extraccion con filtros y paginacion |
| GET | `/events/{job_id}` | Obtener detalle de un evento de extraccion especifico |
| GET | `/events/{job_id}/download` | Descargar resultados del job como CSV |

---

## Extraccion (`/extract`)

| Metodo | Ruta | Descripcion |
|--------|------|-------------|
| POST | `/extract` | Iniciar job de extraccion (SQL Server -> Databricks) en background |

---

## Jobs (`/jobs`)

| Metodo | Ruta | Descripcion |
|--------|------|-------------|
| GET | `/jobs` | Listar todos los jobs de extraccion (con filtros de estado/etapa/periodo) |
| GET | `/jobs/{job_id}` | Obtener estado de un job especifico |
| DELETE | `/jobs/{job_id}` | Cancelar un job en ejecucion |

---

## Pipeline de Calibracion (`/pipeline`)

| Metodo | Ruta | Descripcion |
|--------|------|-------------|
| POST | `/pipeline` | Crear un nuevo pipeline de calibracion para un pais |
| GET | `/pipeline` | Listar pipelines de calibracion (filtrable por pais) |
| GET | `/pipeline/{id}` | Obtener detalle del pipeline |
| POST | `/pipeline/{id}/poll` | Consultar estado de los pasos en Databricks Jobs API |
| PATCH | `/pipeline/{id}/steps/{step_id}` | Actualizar estado de un paso del pipeline |
| PATCH | `/pipeline/{id}/steps/{step_id}/substeps/{name}` | Actualizar estado de un sub-paso |
| DELETE | `/pipeline/{id}` | Eliminar un pipeline |

---

## Sync — Databricks a SQL Server (`/sync`)

| Metodo | Ruta | Descripcion |
|--------|------|-------------|
| POST | `/sync/events` | Enviar evento de sync (INSERT/UPDATE/DELETE de Databricks a SQL Server) |
| GET | `/sync/events` | Listar eventos de sync con filtros |
| GET | `/sync/events/{id}` | Obtener detalle de un evento de sync |
| POST | `/sync/events/{id}/retry` | Reintentar un evento fallido |
| DELETE | `/sync/events/{id}` | Cancelar un evento pendiente |
| GET | `/sync/stats` | Obtener estadisticas de eventos de sync |
| GET | `/sync/poller/status` | Estado del poller en background |
| POST | `/sync/poller/trigger` | Disparar ciclo de polling manualmente |
| WS | `/sync/events/ws` | WebSocket para actualizaciones en tiempo real |

---

## Tags de Versionamiento (`/tags`)

| Metodo | Ruta | Descripcion |
|--------|------|-------------|
| POST | `/tags` | Crear un tag de version para una tabla Delta |
| GET | `/tags` | Listar tags de version (filtrable por tabla/tag) |
| DELETE | `/tags/{tag}` | Eliminar un tag de version |
| GET | `/tags/history/{table}` | Obtener historial de versiones de una tabla Delta con sus tags |

---

## Jobs de Databricks (`/databricks`)

| Metodo | Ruta | Descripcion |
|--------|------|-------------|
| POST | `/databricks/jobs/{job_id}/run` | Ejecutar un job existente en Databricks |
| GET | `/databricks/runs/{run_id}` | Obtener estado y progreso de una ejecucion en Databricks |

---

**Total: 35 endpoints** (33 REST + 1 WebSocket + 1 sonda de arranque) en 10 modulos de rutas.

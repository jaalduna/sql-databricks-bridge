# SQL-Databricks Bridge -- Resumen Ejecutivo

**Servicio**: sql-databricks-bridge v0.1.14
**Equipo**: Technology Solutions LATAM -- Numerator / Kantar
**Fecha**: Febrero 2026

---

## Descripcion General

SQL-Databricks Bridge es un sistema de escritorio compuesto por dos ejecutables independientes que trabajan en conjunto para sincronizar datos entre SQL Server on-premise y Databricks Unity Catalog, y orquestar pipelines de calibracion de panel de consumidores para 8 paises de Latinoamerica (Bolivia, Brasil, CAM, Chile, Colombia, Ecuador, Mexico, Peru).

| Componente | Tipo | Tecnologia | Ejecutable |
|-----------|------|-----------|------------|
| **Frontend** | Aplicacion de escritorio nativa | Tauri 2.x (Rust + React 19 + TypeScript) | Instalador NSIS para Windows (.exe) |
| **Backend** | Servicio API standalone | FastAPI (Python 3.11) compilado con Nuitka | Binario Windows sin dependencia de Python |

Ambos componentes se distribuyen como ejecutables standalone para Windows. No requieren instalacion de runtimes adicionales (ni Node.js, ni Python). El frontend se comunica con el backend via HTTP REST (`/api/v1`).

---

## Funcionalidades

| Funcionalidad | Descripcion |
|--------------|-------------|
| **Sincronizacion SQL Server -> Databricks** | Extraccion paralela de tablas por pais hacia Delta tables en Unity Catalog, con chunking configurable y versionamiento automatico |
| **Calibracion de Panel** | Pipeline de 6 pasos (sync, copy, merge, simulate KPIs, penetracion, CSV) orquestado sobre Databricks Asset Bundles con monitoreo en tiempo real |
| **Monitoreo de Jobs** | Progreso en tiempo real por query individual, tiempos, throughput y deteccion de errores |
| **Historial y Filtros** | Registro completo de ejecuciones con filtros por pais, estado y paginacion server-side |
| **Retry Selectivo** | Reintento individual de queries fallidas sin re-ejecutar el job completo |
| **Descarga de Resultados** | Exportacion CSV de resultados de calibracion |

---

## Diagrama General del Sistema

```mermaid
flowchart TB
    subgraph Desktop["Escritorio del Usuario (Windows)"]
        FE["Frontend Desktop<br/><b>Tauri 2.x</b><br/><i>Instalador NSIS</i>"]
    end

    subgraph Server["Servidor Backend (Windows)"]
        BE["Backend API<br/><b>FastAPI + Nuitka</b><br/><i>Binario standalone</i>"]
    end

    FE -->|"HTTP REST<br/>/api/v1/*"| BE

    subgraph OnPrem["Red Corporativa"]
        SQL1["SQL Server<br/><i>Bolivia</i>"]
        SQL2["SQL Server<br/><i>Chile</i>"]
        SQLN["SQL Server<br/><i>...8 paises</i>"]
    end

    subgraph Cloud["Azure / Databricks"]
        UC["Unity Catalog<br/><i>Delta Tables</i>"]
        JOBS["Databricks Jobs<br/><i>Asset Bundles</i>"]
        VOL["Volumes<br/><i>Staging Parquet</i>"]
    end

    BE -->|"pyodbc<br/>Extraccion chunked"| SQL1 & SQL2 & SQLN
    BE -->|"SDK<br/>Statement Execution API"| UC
    BE -->|"Jobs API<br/>Trigger + Polling"| JOBS
    BE -->|"Files API<br/>Upload Parquet"| VOL

    subgraph Pipeline["Pipeline de Calibracion (6 pasos)"]
        direction LR
        P1["1. Sync Data"]
        P2["2. Copy Bronze"]
        P3["3. Merge"]
        P4["4. KPIs"]
        P5["5. Penetracion"]
        P6["6. CSV"]
        P1 --> P2 --> P3 --> P4 --> P5 --> P6
    end

    BE --> Pipeline
    Pipeline --> JOBS

    classDef desktopNode fill:#c5cae9,stroke:#283593,stroke-width:1px,color:#1a237e
    classDef serverNode fill:#b2dfdb,stroke:#00695c,stroke-width:1px,color:#004d40
    classDef onpremNode fill:#ffe0b2,stroke:#e65100,stroke-width:1px,color:#bf360c
    classDef cloudNode fill:#a5d6a7,stroke:#2e7d32,stroke-width:1px,color:#1b5e20
    classDef pipelineNode fill:#ce93d8,stroke:#6a1b9a,stroke-width:1px,color:#4a148c

    class FE desktopNode
    class BE serverNode
    class SQL1,SQL2,SQLN onpremNode
    class UC,JOBS,VOL cloudNode
    class P1,P2,P3,P4,P5,P6 pipelineNode

    style Desktop fill:#e8f0fe,stroke:#4285f4,stroke-width:2px,color:#1a237e
    style Server fill:#e0f2f1,stroke:#00897b,stroke-width:2px,color:#004d40
    style OnPrem fill:#fef7e0,stroke:#f9ab00,stroke-width:2px,color:#bf360c
    style Cloud fill:#e6f4ea,stroke:#34a853,stroke-width:2px,color:#1b5e20
    style Pipeline fill:#f3e5f5,stroke:#9c27b0,stroke-width:1px,color:#4a148c
```

---

## Flujo Tipico de Uso

```
1. INICIO
   El usuario abre la aplicacion de escritorio e ingresa al sistema.

2. SINCRONIZACION DE DATOS
   En el Dashboard, selecciona un pais y una etapa (ej: Chile - Calibracion).
   Confirma el trigger. La aplicacion navega al detalle del job donde se
   muestra el progreso en tiempo real: queries ejecutandose, filas extraidas,
   throughput por query.

3. CALIBRACION
   En la pagina de Calibracion, selecciona el periodo (ej: Feb 2026),
   verifica disponibilidad de datos (elegibilidad y pesaje) por pais,
   y lanza el pipeline de 6 pasos. La barra de progreso y los indicadores
   por paso se actualizan automaticamente cada 2 segundos.

4. REVISION Y DESCARGA
   Al completar, revisa el detalle expandible de cada paso y descarga
   el CSV con los resultados. Si alguna query fallo, puede reintentar
   solo las fallidas sin re-ejecutar todo el job.

5. HISTORIAL
   En cualquier momento, consulta el historial completo de ejecuciones
   con filtros por pais y estado, y navega al detalle de cualquier job.
```

---

## Stack Tecnologico

| Capa | Frontend | Backend |
|------|----------|---------|
| **Lenguaje** | TypeScript 5.9 + Rust | Python 3.11 |
| **Framework** | React 19 + Tauri 2.x | FastAPI |
| **Estado** | TanStack React Query | In-memory + SQLite |
| **DataFrames** | -- | Polars |
| **Base de datos** | -- | SQL Server (pyodbc), Databricks SDK, SQLite |
| **UI** | Tailwind CSS + Shadcn/ui | -- |
| **Compilacion** | Tauri Build (NSIS) | Nuitka (standalone) |
| **Distribucion** | Instalador .exe Windows | Binario .exe Windows |

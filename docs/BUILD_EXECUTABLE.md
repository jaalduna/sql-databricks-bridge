# Compilación de Ejecutable con Nuitka

Esta guía describe cómo compilar SQL-Databricks Bridge en un ejecutable standalone usando Nuitka.

## Requisitos Previos

### Windows

1. **Python 3.11+**
   ```powershell
   python --version
   ```

2. **Visual Studio Build Tools** (para el compilador C)
   - Descargar desde: https://visualstudio.microsoft.com/visual-cpp-build-tools/
   - Instalar "Desktop development with C++"

3. **Dependencias del proyecto**
   ```powershell
   poetry install --with build
   ```

### Linux

1. **Python 3.11+**
   ```bash
   python3 --version
   ```

2. **GCC y dependencias**
   ```bash
   # Ubuntu/Debian
   sudo apt-get install build-essential python3-dev

   # RHEL/CentOS
   sudo yum groupinstall "Development Tools"
   sudo yum install python3-devel
   ```

3. **Dependencias del proyecto**
   ```bash
   poetry install --with build
   ```

## Compilación

### Método 1: Usando el Script de Build

```powershell
# Windows
cd sql-databricks-bridge
build\build.bat

# Compilar solo el servidor
build\build.bat server

# Compilar como archivo único (más lento al iniciar, más fácil de distribuir)
build\build.bat both --onefile
```

```bash
# Linux/Mac
cd sql-databricks-bridge
python build/build_nuitka.py

# Compilar solo el CLI
python build/build_nuitka.py --target cli

# Compilar como archivo único
python build/build_nuitka.py --onefile
```

### Método 2: Compilación Manual con Nuitka

```powershell
# Activar entorno virtual
poetry shell

# Compilar el servidor
python -m nuitka ^
    --standalone ^
    --follow-imports ^
    --include-package=uvicorn ^
    --include-package=fastapi ^
    --include-package=starlette ^
    --include-package=pydantic ^
    --include-package=polars ^
    --include-package=pyarrow ^
    --include-package=sqlalchemy ^
    --include-package=pyodbc ^
    --include-package=databricks.sdk ^
    --output-dir=dist ^
    --output-filename=sql-databricks-bridge-server ^
    src\sql_databricks_bridge\server.py
```

## Estructura de Salida

Después de la compilación, encontrarás en `dist/`:

```
dist/
├── sql-databricks-bridge-server.dist/   # Carpeta del servidor (standalone)
│   ├── sql-databricks-bridge-server.exe
│   ├── *.dll                             # DLLs necesarias
│   └── ...
├── sql-databricks-bridge.dist/          # Carpeta del CLI (standalone)
│   ├── sql-databricks-bridge.exe
│   └── ...
├── config/                              # Archivos de configuración
│   └── permissions.yaml
└── .env.example                         # Plantilla de configuración
```

Para compilación `--onefile`:

```
dist/
├── sql-databricks-bridge-server.exe     # Ejecutable único del servidor
├── sql-databricks-bridge.exe            # Ejecutable único del CLI
├── config/
└── .env.example
```

## Despliegue

### 1. Preparar el Servidor

```powershell
# Copiar la carpeta dist al servidor de destino
xcopy /E /I dist C:\Services\sql-databricks-bridge

# O copiar los archivos específicos
xcopy /E /I dist\sql-databricks-bridge-server.dist C:\Services\sql-databricks-bridge
copy dist\.env.example C:\Services\sql-databricks-bridge\.env
xcopy /E /I dist\config C:\Services\sql-databricks-bridge\config
```

### 2. Configurar Variables de Entorno

```powershell
# Editar el archivo .env
notepad C:\Services\sql-databricks-bridge\.env
```

Contenido del `.env`:
```ini
# SQL Server
SQLSERVER_HOST=your-server.database.windows.net
SQLSERVER_DATABASE=your_database
SQLSERVER_USERNAME=your_user
SQLSERVER_PASSWORD=your_password

# Databricks
DATABRICKS_HOST=https://your-workspace.azuredatabricks.net
DATABRICKS_TOKEN=your_token
DATABRICKS_CATALOG=your_catalog
DATABRICKS_WAREHOUSE_ID=your_warehouse_id

# API
API_HOST=0.0.0.0
API_PORT=8000
```

### 3. Ejecutar el Servicio

```powershell
cd C:\Services\sql-databricks-bridge

# Ejecutar directamente
sql-databricks-bridge-server.exe --host 0.0.0.0 --port 8000

# O ejecutar desde la carpeta .dist
sql-databricks-bridge-server.dist\sql-databricks-bridge-server.exe --port 8000
```

### 4. Instalar como Servicio de Windows

Usar NSSM (Non-Sucking Service Manager):

```powershell
# Descargar NSSM desde https://nssm.cc/download
# Instalar el servicio
nssm install SQLDatabricksBridge "C:\Services\sql-databricks-bridge\sql-databricks-bridge-server.exe"
nssm set SQLDatabricksBridge AppDirectory "C:\Services\sql-databricks-bridge"
nssm set SQLDatabricksBridge AppParameters "--host 0.0.0.0 --port 8000"
nssm set SQLDatabricksBridge DisplayName "SQL-Databricks Bridge"
nssm set SQLDatabricksBridge Description "Servicio de sincronización SQL Server - Databricks"
nssm set SQLDatabricksBridge Start SERVICE_AUTO_START

# Iniciar el servicio
nssm start SQLDatabricksBridge
```

O usando `sc.exe`:

```powershell
sc create SQLDatabricksBridge binPath= "C:\Services\sql-databricks-bridge\sql-databricks-bridge-server.exe --port 8000" start= auto
sc description SQLDatabricksBridge "SQL-Databricks Bridge Service"
sc start SQLDatabricksBridge
```

## Comandos del CLI

El ejecutable CLI soporta todos los comandos originales:

```powershell
# Ver versión
sql-databricks-bridge.exe version

# Probar conexiones
sql-databricks-bridge.exe test-connection

# Listar queries disponibles
sql-databricks-bridge.exe list-queries --queries-path ./queries

# Extraer datos
sql-databricks-bridge.exe extract ^
    --queries-path ./queries ^
    --config-path ./config ^
    --country Colombia ^
    --destination /Volumes/catalog/schema/volume

# Iniciar servidor (alternativa al ejecutable server)
sql-databricks-bridge.exe serve --port 8000
```

## Solución de Problemas

### Error: "VCRUNTIME140.dll not found"

Instalar Visual C++ Redistributable:
- https://aka.ms/vs/17/release/vc_redist.x64.exe

### Error: "pyodbc.Error: ... Driver not found"

Instalar ODBC Driver for SQL Server:
- https://docs.microsoft.com/sql/connect/odbc/download-odbc-driver-for-sql-server

### Error: "Module not found"

Si falta algún módulo, agregarlo explícitamente al build:
```python
# En build/build_nuitka.py, agregar a get_common_nuitka_args():
"--include-package=modulo_faltante",
```

### El ejecutable es muy grande

El modo standalone incluye todas las dependencias. Para reducir tamaño:

1. Usar `--onefile` (comprime los archivos)
2. Excluir módulos no necesarios:
   ```
   --nofollow-import-to=test
   --nofollow-import-to=pytest
   ```

### El servidor no inicia

1. Verificar que el puerto no esté en uso:
   ```powershell
   netstat -an | findstr :8000
   ```

2. Verificar logs:
   ```powershell
   sql-databricks-bridge-server.exe --port 8000 2>&1 | tee server.log
   ```

3. Verificar variables de entorno:
   ```powershell
   type .env
   ```

## Notas de Rendimiento

- **Primera ejecución**: Puede ser más lenta mientras se descomprimen recursos
- **Modo onefile**: Inicio más lento (~2-5 segundos adicionales)
- **Modo standalone**: Inicio más rápido, pero más archivos para distribuir
- **Memoria**: Similar al script Python original

## Integración con CI/CD

Ejemplo para Azure DevOps:

```yaml
# azure-pipelines-build.yml
trigger:
  branches:
    include:
      - main
  paths:
    include:
      - src/**

pool:
  vmImage: 'windows-latest'

steps:
  - task: UsePythonVersion@0
    inputs:
      versionSpec: '3.11'

  - script: |
      pip install poetry
      poetry install --with build
    displayName: 'Install dependencies'

  - script: |
      poetry run python build/build_nuitka.py --onefile
    displayName: 'Build with Nuitka'

  - task: PublishPipelineArtifact@1
    inputs:
      targetPath: 'dist'
      artifact: 'executable'
    displayName: 'Publish artifact'
```

---

*Última actualización: Enero 2024*

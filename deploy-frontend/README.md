# SQL-Databricks Bridge - Desktop App (Guia de Deploy)

Aplicacion de escritorio para gestionar extracciones SQL Server a Databricks.

## Contenido del paquete

```
deploy-frontend/
  SQL Databricks Bridge_x.x.x_x64-setup.exe   # Instalador Windows (desde GitHub Releases)
  config.json                                   # ← UNICO ARCHIVO A CONFIGURAR
  README.md                                     # Este archivo
```

## Paso 1: Instalar la aplicacion

Ejecutar el instalador `.exe` descargado desde GitHub Releases.
Se instala en `C:\Users\<usuario>\AppData\Local\SQL Databricks Bridge\`.

## Paso 2: Configurar la URL del backend

Copiar el archivo `config.json` a la carpeta donde se instalo la app:

```
C:\Users\<usuario>\AppData\Local\SQL Databricks Bridge\config.json
```

Editar `config.json` con la IP y puerto del servidor backend:

```json
{
  "API_URL": "http://10.200.1.50:8000/api/v1"
}
```

Ejemplos segun el escenario:

| Escenario | Valor de API_URL |
|-----------|-----------------|
| Backend en el mismo equipo | `http://localhost:8000/api/v1` |
| Backend en otro servidor (IP) | `http://10.200.1.50:8000/api/v1` |
| Backend en otro servidor (nombre) | `http://KTCLSQL001:8000/api/v1` |
| Backend en puerto distinto | `http://10.200.1.50:9090/api/v1` |

## Paso 3: Abrir la aplicacion

Ejecutar "SQL Databricks Bridge" desde el menu Inicio o el acceso directo.

## Verificar que funciona

1. Al abrir debe aparecer el dashboard (no una pantalla en blanco)
2. Debe cargar la lista de paises en el selector
3. Si aparece "Network Error", verificar que:
   - La URL en `config.json` es correcta
   - El backend esta corriendo en esa URL
   - No hay firewall bloqueando la conexion

## Cambiar configuracion

Para cambiar la URL del backend:

1. Cerrar la aplicacion
2. Editar `config.json` con la nueva URL
3. Abrir la aplicacion de nuevo

## Notas

- Si `config.json` no existe, la app usa `http://localhost:8000/api/v1` por defecto
- La app se actualiza automaticamente cuando hay nuevas versiones en GitHub Releases

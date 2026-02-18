# SQL-Databricks Bridge - Desktop App (Guia de Deploy)

Aplicacion de escritorio para gestionar extracciones SQL Server a Databricks.

## Contenido del paquete

```
deploy-frontend/
  SQL.Databricks.Bridge_0.1.7_x64-setup.exe   # Instalador Windows
  config.json                                   # Configuracion (editar antes de usar)
  README.md                                     # Este archivo
```

## Paso 1: Instalar la aplicacion

Ejecutar `SQL.Databricks.Bridge_0.1.7_x64-setup.exe`.
Se instala en: `C:\Users\<usuario>\AppData\Local\SQL Databricks Bridge\`

## Paso 2: Configurar la URL del backend

Copiar `config.json` a la carpeta de instalacion:

```
C:\Users\<usuario>\AppData\Local\SQL Databricks Bridge\config.json
```

Abrir `config.json` con un editor de texto y cambiar la URL para que
apunte al servidor donde corre el backend (sql-databricks-bridge.exe):

```json
{
  "API_URL": "http://10.200.1.50:8000/api/v1"
}
```

### Ejemplos

| Escenario | Valor de API_URL |
|-----------|-----------------|
| Backend en el mismo equipo | `http://localhost:8000/api/v1` |
| Backend en otro servidor (IP) | `http://10.200.1.50:8000/api/v1` |
| Backend en otro servidor (nombre) | `http://KTCLSQL001:8000/api/v1` |
| Backend en puerto distinto | `http://10.200.1.50:9090/api/v1` |

**Importante:** La URL debe ser accesible desde el equipo donde se ejecuta
la aplicacion de escritorio.

## Paso 3: Abrir la aplicacion

Ejecutar "SQL Databricks Bridge" desde el menu Inicio o el acceso directo.

## Verificar que funciona

1. Al abrir debe aparecer el dashboard con el selector de paises
2. Si aparece "Network Error":
   - Verificar que la URL en `config.json` es correcta
   - Verificar que el backend esta corriendo
   - Verificar que no hay firewall bloqueando la conexion
   - Verificar que el puerto del backend esta abierto (por defecto 8000)

## Cambiar configuracion

1. Cerrar la aplicacion
2. Editar `config.json` con la nueva URL
3. Abrir la aplicacion de nuevo

No es necesario reinstalar ni reconstruir nada.

## Notas

- Si `config.json` no existe, la app usa `http://localhost:8000/api/v1` por defecto
- La app se actualiza automaticamente cuando hay nuevas versiones en GitHub Releases
- El backend debe estar corriendo para que la app funcione

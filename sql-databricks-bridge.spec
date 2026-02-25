# -*- mode: python ; coding: utf-8 -*-


a = Analysis(
    ['packaging\\entry_point.py'],
    pathex=[],
    binaries=[],
    datas=[],
    hiddenimports=['uvicorn', 'uvicorn.lifespan.on', 'uvicorn.lifespan.off', 'uvicorn.logging', 'uvicorn.loops.auto', 'uvicorn.protocols.http.auto', 'uvicorn.protocols.http.h11_impl', 'uvicorn.protocols.http.httptools_impl', 'uvicorn.protocols.websockets.auto', 'uvicorn.protocols.websockets.wsproto_impl', 'sqlalchemy.dialects.mssql', 'sqlalchemy.dialects.mssql.pyodbc', 'kantar_db_handler', 'kantar_db_handler.configs', 'kantar_db_handler.db_handler', 'kantar_db_handler.sql_alchemy_drv', 'pyodbc', 'polars', 'pyarrow', 'multipart', 'anyio._backends._asyncio', 'pydantic', 'pydantic_settings', 'starlette.responses', 'starlette.routing', 'starlette.middleware', 'starlette.middleware.cors', 'httpx', 'jwt', 'yaml', 'sql_databricks_bridge.api.routes.auth', 'sql_databricks_bridge.api.routes.metadata', 'sql_databricks_bridge.api.routes.trigger', 'sql_databricks_bridge.api.routes.extract', 'sql_databricks_bridge.api.routes.health', 'sql_databricks_bridge.api.routes.jobs', 'sql_databricks_bridge.api.routes.sync', 'sql_databricks_bridge.api.dependencies', 'sql_databricks_bridge.auth.azure_ad', 'sql_databricks_bridge.auth.authorized_users', 'sql_databricks_bridge.auth.audit', 'sql_databricks_bridge.auth.loader', 'sql_databricks_bridge.auth.permissions', 'sql_databricks_bridge.core.stages', 'sql_databricks_bridge.data', 'sql_databricks_bridge.db.jobs_table', 'sql_databricks_bridge.db.local_store', 'sql_databricks_bridge.sync.poller', 'sql_databricks_bridge.sync.operations', 'sql_databricks_bridge.sync.retry'],
    hookspath=['packaging/hooks'],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[],
    noarchive=False,
    optimize=0,
)
pyz = PYZ(a.pure)

exe = EXE(
    pyz,
    a.scripts,
    a.binaries,
    a.datas,
    [],
    name='sql-databricks-bridge',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    upx_exclude=[],
    runtime_tmpdir=None,
    console=True,
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
)

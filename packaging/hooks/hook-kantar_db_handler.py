"""PyInstaller hook for kantar_db_handler.

Ensures config_files/*.json are bundled into the executable
and all submodules are detected as hidden imports.
"""

from PyInstaller.utils.hooks import collect_data_files, collect_submodules

datas = collect_data_files("kantar_db_handler")
hiddenimports = collect_submodules("kantar_db_handler")

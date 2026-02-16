"""PyInstaller hook to include bundled YAML configs from sql_databricks_bridge.data."""

from PyInstaller.utils.hooks import collect_data_files

datas = collect_data_files("sql_databricks_bridge.data")

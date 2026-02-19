// =============================================================================
// SQL-Databricks Bridge - Configuracion del Frontend
// =============================================================================
// Editar este archivo con la URL del servidor backend.
// NO es necesario reconstruir la app, solo editar y refrescar el navegador.

window.__BRIDGE_CONFIG__ = {
  API_URL: "http://localhost:8000/api/v1",
};

window.__APP_CONFIG__ = {
  modules: {
    sync: true,
    calibration: true,
  },
};

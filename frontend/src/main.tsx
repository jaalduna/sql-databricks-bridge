import { StrictMode } from "react"
import { createRoot } from "react-dom/client"
import "./index.css"
import App from "./App.tsx"

async function loadConfig() {
  if ("__TAURI_INTERNALS__" in window) {
    const { invoke } = await import("@tauri-apps/api/core")
    const configStr = await invoke<string>("read_config")
    try {
      const config = JSON.parse(configStr)

      // Apply API_URL to __BRIDGE_CONFIG__
      if (config.API_URL) {
        ;(window as any).__BRIDGE_CONFIG__ = {
          ...(window as any).__BRIDGE_CONFIG__,
          API_URL: config.API_URL,
        }
      }

      // Apply modules to __APP_CONFIG__
      if (config.modules) {
        ;(window as any).__APP_CONFIG__ = {
          ...(window as any).__APP_CONFIG__,
          modules: {
            ...(window as any).__APP_CONFIG__?.modules,
            ...config.modules,
          },
        }
      }
    } catch {
      console.warn("Failed to parse config.json, using defaults")
    }
  }
}

loadConfig().then(() => {
  createRoot(document.getElementById("root")!).render(
    <StrictMode>
      <App />
    </StrictMode>,
  )
})

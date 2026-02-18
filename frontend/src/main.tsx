import { StrictMode } from "react"
import { createRoot } from "react-dom/client"
import "./index.css"
import App from "./App.tsx"

async function loadConfig() {
  if ("__TAURI_INTERNALS__" in window) {
    const { invoke } = await import("@tauri-apps/api/core")
    const configStr = await invoke<string>("read_config")
    try {
      ;(window as any).__BRIDGE_CONFIG__ = JSON.parse(configStr)
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

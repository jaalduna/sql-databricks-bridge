import { lazy } from "react"
import type { ModuleDefinition } from "../types"

export const elegibilidadModule: ModuleDefinition = {
  id: "elegibilidad",
  label: "Elegibilidad",
  navItems: [{ path: "/elegibilidad", label: "Elegibilidad" }],
  routes: [{ path: "/elegibilidad", component: lazy(() => import("./pages/ElegibilidadPage").then(m => ({ default: m.ElegibilidadPage }))) }],
}

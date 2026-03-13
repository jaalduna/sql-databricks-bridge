import { lazy } from "react"
import type { ModuleDefinition } from "../types"

export const calibracionModule: ModuleDefinition = {
  id: "calibration",
  label: "Calibración",
  navItems: [{ path: "/calibration", label: "Calibración" }],
  routes: [{ path: "/calibration", component: lazy(() => import("./pages/CalibrationPage").then(m => ({ default: m.CalibrationPage }))) }],
}

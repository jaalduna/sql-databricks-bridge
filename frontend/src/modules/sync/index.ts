import { lazy } from "react"
import type { ModuleDefinition } from "../types"

export const syncModule: ModuleDefinition = {
  id: "sync",
  label: "Sincronización",
  navItems: [
    { path: "/dashboard", label: "Sincronización" },
    { path: "/history", label: "Historial" },
  ],
  routes: [
    { path: "/dashboard", component: lazy(() => import("./pages/DashboardPage")) },
    { path: "/history", component: lazy(() => import("./pages/HistoryPage")) },
    { path: "/events/:jobId", component: lazy(() => import("./pages/EventDetailPage")) },
  ],
}

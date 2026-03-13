import type { ComponentType } from "react"

export interface ModuleRoute {
  path: string
  component: React.LazyExoticComponent<ComponentType>
}

export interface ModuleNavItem {
  path: string
  label: string
}

export interface ModuleDefinition {
  id: string
  label: string
  navItems: ModuleNavItem[]
  routes: ModuleRoute[]
}

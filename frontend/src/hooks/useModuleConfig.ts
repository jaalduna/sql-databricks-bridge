interface ModuleConfig {
  sync: boolean
  calibration: boolean
}

export function useModuleConfig(): ModuleConfig {
  const config = (window as any).__APP_CONFIG__?.modules
  return {
    sync: config?.sync ?? true,
    calibration: config?.calibration ?? true,
  }
}

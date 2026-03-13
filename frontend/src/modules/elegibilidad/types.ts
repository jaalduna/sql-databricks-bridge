export type EligibilityStepName =
  | 'run_pipeline'
  | 'download_results'
  | 'approve'
  | 'apply_sql'
  | 'download_mordom'
  | 'upload_mordom'
  | 'apply_mordom'
  | 'complete'

export interface EligibilityStep {
  name: EligibilityStepName
  label: string
  status: string
  started_at?: string
  completed_at?: string
}

export interface EligibilityFile {
  file_id: string
  run_id: string
  stage: number
  direction: 'download' | 'upload'
  filename: string
  stored_path: string
  file_size_bytes?: number
  uploaded_by?: string
  created_at: string
}

export type EligibilityStatus =
  | 'pending'
  | 'running'
  | 'results_ready'
  | 'applying_sql'
  | 'mordom_downloading'
  | 'mordom_ready'
  | 'mordom_uploaded'
  | 'applying_mordom'
  | 'ready'
  | 'failed'
  | 'cancelled'

export interface EligibilityRun {
  run_id: string
  country: string
  period: number
  status: EligibilityStatus
  parameters_json?: Record<string, unknown>
  phase1_metrics_json?: Record<string, unknown>
  steps?: EligibilityStep[]
  files?: EligibilityFile[]
  current_step?: string
  started_at?: string
  completed_at?: string
  created_by?: string
  created_at: string
  updated_at: string
  approved_by?: string
  approved_at?: string
  error_message?: string
}

export interface EligibilityRunCreate {
  country: string
  period: number
  parameters?: Record<string, unknown>
}

export interface EligibilityMetrics {
  elegibles_naturales?: number
  donadores_totales?: number
  receptores_totales?: number
  muertos?: number
  inconsistentes?: number
  buen_colaborador?: number
  [key: string]: number | undefined
}

export const STEP_ORDER: EligibilityStepName[] = [
  'run_pipeline',
  'download_results',
  'approve',
  'apply_sql',
  'download_mordom',
  'upload_mordom',
  'apply_mordom',
  'complete',
]

export const STEP_LABELS: Record<EligibilityStepName, string> = {
  run_pipeline: 'Ejecutar Pipeline',
  download_results: 'Descargar Resultados',
  approve: 'Aprobar',
  apply_sql: 'Aplicar a SQL Server',
  download_mordom: 'Descargar MorDom',
  upload_mordom: 'Subir MorDom corregido',
  apply_mordom: 'Aplicar correcciones MorDom',
  complete: 'Listo',
}

/** Statuses that indicate the pipeline is actively processing. */
export const ACTIVE_STATUSES: EligibilityStatus[] = [
  'running',
  'applying_sql',
  'mordom_downloading',
  'applying_mordom',
]

export const STATUS_CONFIG: Record<
  EligibilityStatus,
  { label: string; variant: 'default' | 'secondary' | 'destructive' | 'outline'; className: string }
> = {
  pending: { label: 'Pendiente', variant: 'outline', className: '' },
  running: { label: 'Ejecutando...', variant: 'default', className: 'bg-blue-500 hover:bg-blue-500/80' },
  results_ready: { label: 'Resultados listos', variant: 'default', className: 'bg-amber-500 hover:bg-amber-500/80' },
  applying_sql: { label: 'Aplicando a SQL...', variant: 'default', className: 'bg-blue-500 hover:bg-blue-500/80' },
  mordom_downloading: { label: 'Descargando MorDom...', variant: 'default', className: 'bg-blue-500 hover:bg-blue-500/80' },
  mordom_ready: { label: 'MorDom listo', variant: 'default', className: 'bg-amber-500 hover:bg-amber-500/80' },
  mordom_uploaded: { label: 'MorDom subido', variant: 'default', className: 'bg-amber-500 hover:bg-amber-500/80' },
  applying_mordom: { label: 'Aplicando MorDom...', variant: 'default', className: 'bg-blue-500 hover:bg-blue-500/80' },
  ready: { label: 'Listo', variant: 'default', className: 'bg-green-600 hover:bg-green-600/80' },
  failed: { label: 'Error', variant: 'destructive', className: '' },
  cancelled: { label: 'Cancelado', variant: 'secondary', className: '' },
}

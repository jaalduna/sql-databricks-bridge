export type EligibilityStepName =
  | 'stage1_job'
  | 'stage1_download'
  | 'stage1_upload'
  | 'stage2_job'
  | 'stage2_download'
  | 'stage2_upload'
  | 'finalize'
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

export interface EligibilityRun {
  run_id: string
  country: string
  period: number
  status: 'pending' | 'stage1_running' | 'stage1_ready' | 'stage1_uploaded' | 'stage2_running' | 'stage2_ready' | 'stage2_uploaded' | 'finalized' | 'failed' | 'cancelled'
  parameters_json?: string
  phase1_metrics_json?: string
  steps?: EligibilityStep[]
  files?: EligibilityFile[]
  current_step?: string
  started_at?: string
  completed_at?: string
  created_by?: string
  created_at: string
  updated_at: string
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
  'stage1_job',
  'stage1_download',
  'stage1_upload',
  'stage2_job',
  'stage2_download',
  'stage2_upload',
  'finalize',
  'complete',
]

export const STEP_LABELS: Record<EligibilityStepName, string> = {
  stage1_job: 'Fase 1 - Ejecucion',
  stage1_download: 'Fase 1 - Descarga',
  stage1_upload: 'Fase 1 - Subida',
  stage2_job: 'Fase 2 - Ejecucion',
  stage2_download: 'Fase 2 - Descarga',
  stage2_upload: 'Fase 2 - Subida',
  finalize: 'Finalizacion',
  complete: 'Completo',
}

export const STATUS_CONFIG: Record<
  EligibilityRun['status'],
  { label: string; variant: 'default' | 'secondary' | 'destructive' | 'outline'; className: string }
> = {
  pending: { label: 'Pendiente', variant: 'outline', className: '' },
  stage1_running: { label: 'Fase 1 en curso', variant: 'default', className: 'bg-blue-500 hover:bg-blue-500/80' },
  stage1_ready: { label: 'Descarga Fase 1', variant: 'default', className: 'bg-amber-500 hover:bg-amber-500/80' },
  stage1_uploaded: { label: 'Fase 1 subida', variant: 'default', className: 'bg-amber-500 hover:bg-amber-500/80' },
  stage2_running: { label: 'Fase 2 en curso', variant: 'default', className: 'bg-blue-500 hover:bg-blue-500/80' },
  stage2_ready: { label: 'Descarga Fase 2', variant: 'default', className: 'bg-amber-500 hover:bg-amber-500/80' },
  stage2_uploaded: { label: 'Fase 2 subida', variant: 'default', className: 'bg-amber-500 hover:bg-amber-500/80' },
  finalized: { label: 'Finalizado', variant: 'default', className: 'bg-green-600 hover:bg-green-600/80' },
  failed: { label: 'Error', variant: 'destructive', className: '' },
  cancelled: { label: 'Cancelado', variant: 'secondary', className: '' },
}

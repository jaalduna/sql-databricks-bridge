/** Job execution status. */
export type JobStatus = "pending" | "running" | "completed" | "failed" | "cancelled"

/** Calibration pipeline step names (ordered). */
export type CalibrationStepName =
  | "sync_data"
  | "copy_to_calibration"
  | "merge_data"
  | "simulate_kpis"
  | "calculate_penetration"
  | "download_csv"

/** Human-readable labels for each calibration step. */
export const CALIBRATION_STEP_LABELS: Record<CalibrationStepName, string> = {
  sync_data: "Sync Data",
  copy_to_calibration: "Copy to Calibration",
  merge_data: "Merge Data",
  simulate_kpis: "Simulate KPIs",
  calculate_penetration: "Calculate Penetration",
  download_csv: "Download CSV",
}

/** Ordered list of calibration steps for iteration. */
export const CALIBRATION_STEPS: CalibrationStepName[] = [
  "sync_data",
  "copy_to_calibration",
  "merge_data",
  "simulate_kpis",
  "calculate_penetration",
  "download_csv",
]

/** Status of a single Databricks task within a multi-task job run. */
export interface DatabricksTaskStatus {
  task_key: string
  status: JobStatus
  started_at: string | null
  completed_at: string | null
  error: string | null
}

/** Status of a single calibration sub-step. */
export interface CalibrationStep {
  name: CalibrationStepName
  status: JobStatus
  started_at: string | null
  completed_at: string | null
  error: string | null
  tasks?: DatabricksTaskStatus[]
}

/** Aggregation level options for calibration */
export interface AggregationOptions {
  region: boolean
  nivel_2: boolean
}

/** Global calibration configuration applied to all countries */
export interface CalibrationConfig {
  aggregations: AggregationOptions
  row_limit: number | null
  lookback_months: number | null
}

export const DEFAULT_CALIBRATION_CONFIG: CalibrationConfig = {
  aggregations: { region: false, nivel_2: false },
  row_limit: null,
  lookback_months: null,
}

/** Data availability for a country+period */
export interface DataAvailability {
  elegibilidad: boolean
  pesaje: boolean
}

/** Response from GET /metadata/data-availability */
export interface DataAvailabilityResponse {
  period: string
  countries: Record<string, DataAvailability>
}

/** User profile returned by GET /auth/me */
export interface UserInfo {
  email: string
  name: string
  roles: string[]
  countries: string[]
}

/** Request body for POST /trigger */
export interface TriggerRequest {
  country: string
  stage: string
  queries?: string[] | null
  lookback_months?: number | null
  row_limit?: number | null
  period?: string | null
  aggregations?: AggregationOptions
  skip_sync?: boolean
  skip_copy?: boolean
}

/** Response from POST /trigger */
export interface TriggerResponse {
  job_id: string
  status: "pending"
  country: string
  stage: string
  tag: string
  queries: string[]
  queries_count: number
  created_at: string
  triggered_by: string
}

/** Single query result within a job */
export interface QueryResult {
  query_name: string
  status: JobStatus
  rows_extracted: number
  estimated_rows: number
  rows_downloaded: number
  table_name: string | null
  duration_seconds: number
  error: string | null
  started_at: string | null
}

/** Summary of a job (in event list) */
export interface EventSummary {
  job_id: string
  status: JobStatus
  country: string
  stage: string
  tag: string
  period?: string
  queries_total: number
  queries_completed: number
  queries_failed: number
  created_at: string
  started_at: string | null
  completed_at: string | null
  triggered_by: string
  error: string | null
  current_query: string | null
  failed_queries: string[]
  running_queries: string[]
  queries_running: number
  total_rows_extracted: number
  steps?: CalibrationStep[]
  current_step?: CalibrationStepName | null
}

/** Detailed job info including per-query results */
export interface EventDetail extends EventSummary {
  results: QueryResult[]
}

/** Paginated event list response */
export interface EventListResponse {
  items: EventSummary[]
  total: number
  limit: number
  offset: number
}

/** Country metadata from GET /metadata/countries */
export interface CountryInfo {
  code: string
  queries: string[]
  queries_count: number
  type: "country" | "server"
}

export interface CountriesResponse {
  countries: CountryInfo[]
}

/** Stage metadata from GET /metadata/stages */
export interface StageInfo {
  code: string
  name: string
}

export interface StagesResponse {
  stages: StageInfo[]
}

/** Standard API error response */
export interface ApiError {
  error: string
  message: string
}

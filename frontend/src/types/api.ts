// Import calibration types (used in interfaces below + re-exported for backward compat)
import type {
  CalibrationStepName,
  DatabricksTaskStatus,
  CalibrationStep,
  AggregationOptions,
  CalibrationConfig,
  DataAvailability,
  DataAvailabilityResponse,
} from "@/modules/calibracion/types"
import {
  CALIBRATION_STEP_LABELS,
  CALIBRATION_STEPS,
  DEFAULT_CALIBRATION_CONFIG,
} from "@/modules/calibracion/types"

export type {
  CalibrationStepName,
  DatabricksTaskStatus,
  CalibrationStep,
  AggregationOptions,
  CalibrationConfig,
  DataAvailability,
  DataAvailabilityResponse,
}
export {
  CALIBRATION_STEP_LABELS,
  CALIBRATION_STEPS,
  DEFAULT_CALIBRATION_CONFIG,
}

/** Job execution status. */
export type JobStatus = "pending" | "running" | "completed" | "failed" | "cancelled"

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

/** Last sync entry for a country from GET /metadata/last-sync */
export interface LastSyncEntry {
  country: string
  completed_at: string
  job_id: string
  stage: string
}

export interface LastSyncResponse {
  countries: Record<string, LastSyncEntry>
}

/** Last elegibilidad entry for a country from GET /metadata/last-elegibilidad */
export interface LastElegibilidadEntry {
  country: string
  completed_at: string
  run_id: string
  period: number
}

export interface LastElegibilidadResponse {
  countries: Record<string, LastElegibilidadEntry>
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

/** Info about a Delta table available for download */
export interface TableInfo {
  full_name: string
  catalog: string
  schema_name: string
  table_name: string
}

/** Response from GET /events/{job_id}/tables */
export interface TablesResponse {
  job_id: string
  country: string
  tables: TableInfo[]
}

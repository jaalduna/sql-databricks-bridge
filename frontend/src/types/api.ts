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
  table_name: string | null
  duration_seconds: number
  error: string | null
}

/** Summary of a job (in event list) */
export interface EventSummary {
  job_id: string
  status: JobStatus
  country: string
  stage: string
  tag: string
  queries_total: number
  queries_completed: number
  queries_failed: number
  created_at: string
  started_at: string | null
  completed_at: string | null
  triggered_by: string
  error: string | null
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

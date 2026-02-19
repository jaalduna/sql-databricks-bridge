import axios from "axios"
import type {
  ApiError,
  CountriesResponse,
  DataAvailabilityResponse,
  EventDetail,
  EventListResponse,
  StagesResponse,
  TriggerRequest,
  TriggerResponse,
  UserInfo,
} from "@/types/api"

const BASE_URL =
  (window as any).__BRIDGE_CONFIG__?.API_URL ??
  import.meta.env.VITE_BRIDGE_API_URL ??
  "http://localhost:8000/api/v1"

export const api = axios.create({
  baseURL: BASE_URL,
  headers: { "Content-Type": "application/json" },
})

/** Set the MSAL token provider as a request interceptor. */
export function setTokenProvider(provider: () => Promise<string>) {
  api.interceptors.request.use(async (config) => {
    const token = await provider()
    config.headers.Authorization = `Bearer ${token}`
    return config
  })
}

/** Transform Axios errors into our ApiError shape. */
api.interceptors.response.use(
  (response) => response,
  (error) => {
    if (axios.isAxiosError(error) && error.response?.data) {
      const apiError: ApiError = {
        error: error.response.data.error ?? "unknown",
        message: error.response.data.message ?? error.message,
      }
      return Promise.reject(apiError)
    }
    return Promise.reject({
      error: "network_error",
      message: error.message ?? "Network error",
    } satisfies ApiError)
  },
)

// -- Auth --

export function getMe() {
  return api.get<UserInfo>("/auth/me").then((r) => r.data)
}

// -- Trigger --

export function triggerSync(body: TriggerRequest) {
  return api.post<TriggerResponse>("/trigger", body).then((r) => r.data)
}

/** Alias for triggerSync for calibration_frontend compatibility. */
export const triggerCalibration = triggerSync

// -- Events --

export function getEvents(params?: {
  country?: string
  status?: string
  stage?: string
  period?: string
  limit?: number
  offset?: number
}) {
  return api.get<EventListResponse>("/events", { params }).then((r) => r.data)
}

export function getEvent(jobId: string) {
  return api.get<EventDetail>(`/events/${jobId}`).then((r) => r.data)
}

export function cancelJob(jobId: string) {
  return api.post<EventDetail>(`/events/${jobId}/cancel`).then((r) => r.data)
}

// -- Metadata --

export function getCountries() {
  return api.get<CountriesResponse>("/metadata/countries").then((r) => r.data)
}

export function getStages() {
  return api.get<StagesResponse>("/metadata/stages").then((r) => r.data)
}

export function getDataAvailability(period: string) {
  return api
    .get<DataAvailabilityResponse>("/metadata/data-availability", { params: { period } })
    .then((r) => r.data)
}

// -- Downloads --

export async function downloadCSV(jobId: string): Promise<void> {
  const response = await api.get(`/events/${jobId}/download`, { responseType: "blob" })
  const url = URL.createObjectURL(new Blob([response.data]))
  const a = document.createElement("a")
  a.href = url
  a.download = `calibration-${jobId}.csv`
  a.click()
  URL.revokeObjectURL(url)
}

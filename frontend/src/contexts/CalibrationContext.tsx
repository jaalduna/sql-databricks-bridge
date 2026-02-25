import { createContext, useContext, useState, useCallback, useEffect } from "react"
import type { ReactNode } from "react"
import { getEvent } from "@/lib/api"

const STORAGE_KEY = "bridge:calibration-job-ids"
const TERMINAL_STATUSES = new Set(["completed", "failed", "cancelled"])

function loadFromStorage(): Record<string, string | null> {
  try {
    const raw = localStorage.getItem(STORAGE_KEY)
    if (raw === null) return {}
    return JSON.parse(raw) as Record<string, string | null>
  } catch {
    console.warn("CalibrationContext: failed to parse localStorage entry, falling back to empty state")
    return {}
  }
}

function saveToStorage(map: Record<string, string | null>): void {
  try {
    localStorage.setItem(STORAGE_KEY, JSON.stringify(map))
  } catch {
    console.warn("CalibrationContext: failed to persist job IDs to localStorage")
  }
}

interface CalibrationContextValue {
  getJobId: (country: string) => string | null
  setJobId: (country: string, jobId: string) => void
  clearJobId: (country: string) => void
}

const CalibrationContext = createContext<CalibrationContextValue | null>(null)

export function CalibrationProvider({ children }: { children: ReactNode }) {
  const [jobIds, setJobIds] = useState<Record<string, string | null>>(loadFromStorage)

  const getJobId = useCallback(
    (country: string) => jobIds[country] ?? null,
    [jobIds],
  )

  const setJobId = useCallback((country: string, jobId: string) => {
    setJobIds((prev) => {
      const next = { ...prev, [country]: jobId }
      saveToStorage(next)
      return next
    })
  }, [])

  const clearJobId = useCallback((country: string) => {
    setJobIds((prev) => {
      const next = { ...prev, [country]: null }
      saveToStorage(next)
      return next
    })
  }, [])

  // S10-002: On mount, validate rehydrated job IDs against the backend.
  // Terminal or missing jobs are cleared so stale entries don't persist.
  useEffect(() => {
    const initialJobIds = loadFromStorage()
    const entries = Object.entries(initialJobIds).filter(
      (entry): entry is [string, string] => entry[1] !== null,
    )
    if (entries.length === 0) return

    for (const [country, jobId] of entries) {
      getEvent(jobId)
        .then((event) => {
          if (TERMINAL_STATUSES.has(event.status)) {
            setJobIds((prev) => {
              const next = { ...prev, [country]: null }
              saveToStorage(next)
              return next
            })
          }
        })
        .catch((err: unknown) => {
          // 404 means the job no longer exists; clear it
          const status =
            typeof err === "object" && err !== null && "error" in err
              ? (err as { error: string }).error
              : null
          const isNotFound =
            status === "not_found" ||
            (typeof err === "object" &&
              err !== null &&
              "response" in err &&
              (err as { response?: { status?: number } }).response?.status === 404)

          if (isNotFound) {
            setJobIds((prev) => {
              const next = { ...prev, [country]: null }
              saveToStorage(next)
              return next
            })
          }
        })
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [])

  return (
    <CalibrationContext.Provider value={{ getJobId, setJobId, clearJobId }}>
      {children}
    </CalibrationContext.Provider>
  )
}

export function useCalibrationContext(): CalibrationContextValue {
  const ctx = useContext(CalibrationContext)
  if (!ctx) {
    throw new Error("useCalibrationContext must be used within a CalibrationProvider")
  }
  return ctx
}

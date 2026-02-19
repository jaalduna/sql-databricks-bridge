import { createContext, useContext, useState, useCallback } from "react"
import type { ReactNode } from "react"

interface CalibrationContextValue {
  getJobId: (country: string) => string | null
  setJobId: (country: string, jobId: string) => void
  clearJobId: (country: string) => void
}

const CalibrationContext = createContext<CalibrationContextValue | null>(null)

export function CalibrationProvider({ children }: { children: ReactNode }) {
  const [jobIds, setJobIds] = useState<Record<string, string | null>>({})

  const getJobId = useCallback(
    (country: string) => jobIds[country] ?? null,
    [jobIds],
  )

  const setJobId = useCallback((country: string, jobId: string) => {
    setJobIds((prev) => ({ ...prev, [country]: jobId }))
  }, [])

  const clearJobId = useCallback((country: string) => {
    setJobIds((prev) => ({ ...prev, [country]: null }))
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

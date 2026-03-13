import { useState } from "react"
import { PeriodSelector } from "@/components/PeriodSelector"
import { CalibrationConfigDialog } from "../components/CalibrationConfigDialog"
import { CountryCard } from "../components/CountryCard"
import { Skeleton } from "@/components/ui/skeleton"
import { useCountries } from "@/hooks/useCountries"
import { useDataAvailability } from "../hooks/useDataAvailability"
import { useLastSync } from "@/hooks/useLastSync"
import { useLastCalibration } from "../hooks/useLastCalibration"
import type { DataAvailability, CalibrationConfig } from "../types"
import { DEFAULT_CALIBRATION_CONFIG } from "../types"

function currentPeriod(): string {
  // Default to the previous month: January is calibrated in February, etc.
  const now = new Date()
  const prev = new Date(now.getFullYear(), now.getMonth() - 1, 1)
  const yyyy = prev.getFullYear()
  const mm = String(prev.getMonth() + 1).padStart(2, "0")
  return `${yyyy}${mm}`
}

const NO_DATA: DataAvailability = { elegibilidad: false, pesaje: false }

export function CalibrationPage() {
  const [period, setPeriod] = useState(currentPeriod)
  const [calibrationConfig, setCalibrationConfig] = useState<CalibrationConfig>(DEFAULT_CALIBRATION_CONFIG)
  const { data, isLoading, error } = useCountries()
  const { data: availability, isLoading: availabilityLoading } = useDataAvailability(period)
  const { data: lastSync } = useLastSync()
  const { data: lastCalibration } = useLastCalibration()

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-bold tracking-tight">Calibración</h1>
          <p className="text-sm text-muted-foreground">
            Dispara y monitorea jobs de calibración por país
          </p>
        </div>
        <div className="flex items-center gap-2">
          <PeriodSelector value={period} onChange={setPeriod} />
          <CalibrationConfigDialog config={calibrationConfig} onChange={setCalibrationConfig} />
        </div>
      </div>

      {/* Country grid */}
      {isLoading && (
        <div className="grid gap-5 sm:grid-cols-2 lg:grid-cols-3 xl:grid-cols-4">
          {Array.from({ length: 6 }).map((_, i) => (
            <Skeleton key={i} className="h-48 rounded-xl" />
          ))}
        </div>
      )}

      {error && (
        <div className="rounded-lg border border-destructive/50 bg-destructive/10 p-4 text-sm text-destructive">
          Failed to load countries: {(error as Error).message}
        </div>
      )}

      {data && (
        <div className="grid gap-5 sm:grid-cols-2 lg:grid-cols-3 xl:grid-cols-4">
          {data.countries.filter((c) => c.type === "country").map((c) => (
            <CountryCard
              key={c.code}
              country={c}
              period={period}
              availability={availability?.[c.code] ?? NO_DATA}
              availabilityLoading={availabilityLoading}
              calibrationConfig={calibrationConfig}
              lastSyncDate={lastSync?.[c.code]?.completed_at ?? null}
              lastCalibrationDate={lastCalibration?.[c.code]?.completed_at ?? null}
            />
          ))}
        </div>
      )}
    </div>
  )
}

import { useState } from "react"
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
import { Button } from "@/components/ui/button"
import {
  AlertDialog,
  AlertDialogAction,
  AlertDialogCancel,
  AlertDialogContent,
  AlertDialogDescription,
  AlertDialogFooter,
  AlertDialogHeader,
  AlertDialogTitle,
  AlertDialogTrigger,
} from "@/components/ui/alert-dialog"
import { Badge } from "@/components/ui/badge"
import { Checkbox } from "@/components/ui/checkbox"
import { Loader2, Eye, Square } from "lucide-react"
import { toast } from "sonner"
import { cn } from "@/lib/utils"
import { DataAvailabilityBadge } from "./DataAvailabilityBadge"
import { CalibrationProgress } from "./CalibrationProgress"
import { CalibrationDetailModal } from "./CalibrationDetailModal"
import { CalibrationHistoryModal } from "./CalibrationHistoryModal"
import { useCalibration } from "../hooks/useCalibration"
import type { CountryInfo } from "@/types/api"
import type { DataAvailability, CalibrationConfig } from "../types"

function formatRelativeTime(isoDate: string): string {
  const diff = Date.now() - new Date(isoDate).getTime()
  const minutes = Math.floor(diff / 60000)
  if (minutes < 1) return "just now"
  if (minutes < 60) return `${minutes}m ago`
  const hours = Math.floor(minutes / 60)
  if (hours < 24) return `${hours}h ago`
  const days = Math.floor(hours / 24)
  return `${days}d ago`
}

const COUNTRY_FLAGS: Record<string, string> = {
  argentina: "\u{1F1E6}\u{1F1F7}",
  bolivia: "\u{1F1E7}\u{1F1F4}",
  brasil: "\u{1F1E7}\u{1F1F7}",
  brazil: "\u{1F1E7}\u{1F1F7}",
  cam: "\u{1F30E}",
  chile: "\u{1F1E8}\u{1F1F1}",
  colombia: "\u{1F1E8}\u{1F1F4}",
  ecuador: "\u{1F1EA}\u{1F1E8}",
  mexico: "\u{1F1F2}\u{1F1FD}",
  peru: "\u{1F1F5}\u{1F1EA}",
}

const COUNTRY_LABELS: Record<string, string> = {
  argentina: "Argentina",
  bolivia: "Bolivia",
  brasil: "Brasil",
  brazil: "Brazil",
  cam: "CAM",
  chile: "Chile",
  colombia: "Colombia",
  ecuador: "Ecuador",
  mexico: "Mexico",
  peru: "Peru",
}

interface CountryCardProps {
  country: CountryInfo
  period: string
  availability: DataAvailability
  availabilityLoading: boolean
  calibrationConfig: CalibrationConfig
  lastSyncDate: string | null
  lastCalibrationDate: string | null
}

export function CountryCard({ country, period, availability, availabilityLoading, calibrationConfig, lastSyncDate, lastCalibrationDate }: CountryCardProps) {
  const [showDetail, setShowDetail] = useState(false)
  const [showHistory, setShowHistory] = useState(false)
  const [skipSync, setSkipSync] = useState(false)
  const [skipCopy, setSkipCopy] = useState(false)
  const { job, isPending, isCancelling, startCalibration, cancelCalibration } = useCalibration(country.code, "calibracion")

  const isRunning = job?.status === "running" || job?.status === "pending"
  const isCompleted = job?.status === "completed"
  const isFailed = job?.status === "failed"
  const isCancelled = job?.status === "cancelled"
  const canTrigger = availability.elegibilidad && !isRunning && !isPending

  const countryLabel = COUNTRY_LABELS[country.code] ?? country.code
  const flag = COUNTRY_FLAGS[country.code] ?? ""

  return (
    <>
      <Card
        className="cursor-pointer hover:shadow-md transition-shadow"
        role="button"
        tabIndex={0}
        aria-label={`View calibration history for ${countryLabel}`}
        onClick={() => setShowHistory(true)}
        onKeyDown={(e) => { if (e.key === "Enter" || e.key === " ") { e.preventDefault(); setShowHistory(true) } }}
      >
        <CardHeader className="pb-2">
          <div className="space-y-1.5">
            <CardTitle className="text-lg whitespace-nowrap">
              {flag && <span className="mr-2">{flag}</span>}
              {countryLabel}
            </CardTitle>
            <div className="flex gap-2">
              <Badge
                variant="outline"
                className={cn(
                  "text-xs",
                  lastSyncDate ? "text-muted-foreground" : "text-amber-600 border-amber-300 bg-amber-50",
                )}
                title={lastSyncDate ? `Sync: ${lastSyncDate.replace("T", " ").slice(0, 16)}` : undefined}
              >
                Sync: {lastSyncDate ? formatRelativeTime(lastSyncDate) : "never"}
              </Badge>
              <Badge
                variant="outline"
                className={cn(
                  "text-xs",
                  lastCalibrationDate ? "text-muted-foreground" : "text-amber-600 border-amber-300 bg-amber-50",
                )}
                title={lastCalibrationDate ? `Cal: ${lastCalibrationDate.replace("T", " ").slice(0, 16)}` : undefined}
              >
                Cal: {lastCalibrationDate ? formatRelativeTime(lastCalibrationDate) : "never"}
              </Badge>
            </div>
          </div>
        </CardHeader>
        <CardContent className="space-y-2">
          {/* Data availability */}
          <div className="flex gap-4">
            <DataAvailabilityBadge label="Elegibilidad" available={availability.elegibilidad} loading={availabilityLoading} />
            <DataAvailabilityBadge label="Pesaje" available={availability.pesaje} loading={availabilityLoading} />
          </div>

          {/* Progress bar with step indicators */}
          {job && (isRunning || isCompleted || isFailed || isCancelled) && (
            <CalibrationProgress job={job} />
          )}

          {/* Status badges */}
          {isCompleted && (
            <Badge variant="default" className="bg-green-600">
              Completed
            </Badge>
          )}
          {isFailed && <Badge variant="destructive">Failed</Badge>}
          {isCancelled && <Badge variant="secondary">Cancelled</Badge>}

          {/* Actions */}
          <div onClick={(e) => e.stopPropagation()}>
            <div className="flex gap-2">
              <AlertDialog>
                <AlertDialogTrigger asChild>
                  <Button
                    size="sm"
                    disabled={!canTrigger}
                    className="flex-1"
                    title={!canTrigger ? "Data extraction (Elegibilidad) required before calibration" : undefined}
                  >
                    {isPending || isRunning ? (
                      <>
                        <Loader2 className="mr-2 h-4 w-4 animate-spin" />
                        {isPending ? "Starting..." : "Running..."}
                      </>
                    ) : (
                      "Calibrar"
                    )}
                  </Button>
                </AlertDialogTrigger>
                <AlertDialogContent>
                  <AlertDialogHeader>
                    <AlertDialogTitle>Confirm Calibration</AlertDialogTitle>
                    <AlertDialogDescription asChild>
                      <div className="text-sm text-muted-foreground">
                        <p>
                          Start calibration for{" "}
                          <strong>{countryLabel}</strong> period{" "}
                          <strong>{period}</strong>?
                        </p>
                        {(calibrationConfig.aggregations.region || calibrationConfig.aggregations.nivel_2) && (
                          <p className="mt-2">
                            Aggregations:{" "}
                            {[calibrationConfig.aggregations.region && "Region", calibrationConfig.aggregations.nivel_2 && "Nivel 2"]
                              .filter(Boolean)
                              .join(", ")}
                          </p>
                        )}
                        {(calibrationConfig.row_limit != null || calibrationConfig.lookback_months != null) && (
                          <p className="mt-2">
                            Overrides:{" "}
                            {[
                              calibrationConfig.row_limit != null && `Top ${calibrationConfig.row_limit} rows`,
                              calibrationConfig.lookback_months != null && `${calibrationConfig.lookback_months} months lookback`,
                            ]
                              .filter(Boolean)
                              .join(", ")}
                          </p>
                        )}
                      </div>
                    </AlertDialogDescription>
                  </AlertDialogHeader>
                  <label className="flex items-center gap-2 text-sm">
                    <Checkbox
                      checked={skipSync}
                      onCheckedChange={(v) => setSkipSync(v === true)}
                    />
                    <span>Skip data sync</span>
                  </label>
                  <p className="text-xs text-muted-foreground -mt-2">
                    Use when data is already synced for this country
                  </p>
                  <label className="flex items-center gap-2 text-sm">
                    <Checkbox
                      checked={skipCopy}
                      onCheckedChange={(v) => setSkipCopy(v === true)}
                    />
                    <span>Skip data copy</span>
                  </label>
                  <p className="text-xs text-muted-foreground -mt-2">
                    Use when data is already in the calibration catalog
                  </p>
                  <AlertDialogFooter>
                    <AlertDialogCancel>Cancel</AlertDialogCancel>
                    <AlertDialogAction onClick={() => {
                      startCalibration({
                        period,
                        aggregations: calibrationConfig.aggregations,
                        row_limit: calibrationConfig.row_limit,
                        lookback_months: calibrationConfig.lookback_months,
                        skip_sync: skipSync,
                        skip_copy: skipCopy,
                      }).then(() => {
                        toast.success(`Calibration started for ${countryLabel}`);
                      }).catch((err: unknown) => {
                        const message = (err as { message?: string })?.message ?? "Unknown error";
                        toast.error(`Failed to start calibration for ${countryLabel}: ${message}`);
                      });
                    }}>
                      Confirm
                    </AlertDialogAction>
                  </AlertDialogFooter>
                </AlertDialogContent>
              </AlertDialog>

              {isRunning && (
                <AlertDialog>
                  <AlertDialogTrigger asChild>
                    <Button
                      size="sm"
                      variant="destructive"
                      aria-label="Stop calibration"
                      disabled={isCancelling}
                    >
                      {isCancelling ? (
                        <Loader2 className="h-4 w-4 animate-spin" />
                      ) : (
                        <Square className="h-4 w-4" />
                      )}
                    </Button>
                  </AlertDialogTrigger>
                  <AlertDialogContent>
                    <AlertDialogHeader>
                      <AlertDialogTitle>Stop Calibration</AlertDialogTitle>
                      <AlertDialogDescription>
                        Stop the running calibration for <strong>{countryLabel}</strong>? This will cancel all pending steps.
                      </AlertDialogDescription>
                    </AlertDialogHeader>
                    <AlertDialogFooter>
                      <AlertDialogCancel>Keep Running</AlertDialogCancel>
                      <AlertDialogAction
                        className="bg-destructive text-destructive-foreground hover:bg-destructive/90"
                        onClick={() => {
                          cancelCalibration()
                          toast.info(`Calibration stopped for ${countryLabel}`)
                        }}
                      >
                        Stop
                      </AlertDialogAction>
                    </AlertDialogFooter>
                  </AlertDialogContent>
                </AlertDialog>
              )}

              {job && (
                <Button
                  size="sm"
                  variant="outline"
                  aria-label="View calibration details"
                  onClick={() => setShowDetail(true)}
                >
                  <Eye className="h-4 w-4" />
                </Button>
              )}
            </div>
          </div>
        </CardContent>
      </Card>

      <CalibrationDetailModal
        job={job}
        open={showDetail}
        onClose={() => setShowDetail(false)}
      />
      <CalibrationHistoryModal
        country={country.code}
        countryLabel={countryLabel}
        countryFlag={flag}
        open={showHistory}
        onClose={() => setShowHistory(false)}
      />
    </>
  )
}

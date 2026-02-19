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
import { DataAvailabilityBadge } from "./DataAvailabilityBadge"
import { CalibrationProgress } from "./CalibrationProgress"
import { CalibrationDetailModal } from "./CalibrationDetailModal"
import { useCalibration } from "@/hooks/useCalibration"
import type { CountryInfo, DataAvailability, CalibrationConfig } from "@/types/api"

const COUNTRY_LABELS: Record<string, string> = {
  argentina: "Argentina",
  bolivia: "Bolivia",
  brasil: "Brasil",
  brazil: "Brazil",
  cam: "Centroamerica",
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
  calibrationConfig: CalibrationConfig
}

export function CountryCard({ country, period, availability, calibrationConfig }: CountryCardProps) {
  const [showDetail, setShowDetail] = useState(false)
  const [skipSync, setSkipSync] = useState(false)
  const { job, isPending, isCancelling, startCalibration, cancelCalibration } = useCalibration(country.code, period)

  const isRunning = job?.status === "running" || job?.status === "pending"
  const isCompleted = job?.status === "completed"
  const isFailed = job?.status === "failed"
  const isCancelled = job?.status === "cancelled"
  const canTrigger = availability.elegibilidad && !isRunning && !isPending

  const countryLabel = COUNTRY_LABELS[country.code] ?? country.code

  return (
    <>
      <Card>
        <CardHeader className="pb-3">
          <div className="flex items-center justify-between">
            <CardTitle className="text-base">
              {countryLabel}
            </CardTitle>
            <Badge variant="outline" className="text-xs">
              {country.queries_count} queries
            </Badge>
          </div>
        </CardHeader>
        <CardContent className="space-y-3">
          {/* Data availability */}
          <div className="flex gap-4">
            <DataAvailabilityBadge label="Elegibilidad" available={availability.elegibilidad} />
            <DataAvailabilityBadge label="Pesaje" available={availability.pesaje} />
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
                        <strong>{period}</strong>? This will trigger{" "}
                        {country.queries_count} queries.
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
                <AlertDialogFooter>
                  <AlertDialogCancel>Cancel</AlertDialogCancel>
                  <AlertDialogAction onClick={() => {
                    startCalibration({
                      aggregations: calibrationConfig.aggregations,
                      row_limit: calibrationConfig.row_limit,
                      lookback_months: calibrationConfig.lookback_months,
                      skip_sync: skipSync,
                    });
                    toast.success(`Calibration started for ${countryLabel}`);
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
        </CardContent>
      </Card>

      <CalibrationDetailModal
        job={job}
        open={showDetail}
        onClose={() => setShowDetail(false)}
      />
    </>
  )
}

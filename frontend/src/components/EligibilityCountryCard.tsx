import { useState } from "react"
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
import { Button } from "@/components/ui/button"
import { Badge } from "@/components/ui/badge"
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
import { CheckCircle2, Eye, Loader2, Square } from "lucide-react"

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
import { toast } from "sonner"
import { useEligibilityRuns, useCreateEligibilityRun, useExecuteEligibility, useCancelEligibility } from "@/hooks/useEligibility"
import { EligibilityProgress } from "@/components/EligibilityProgress"
import { EligibilityDetailModal } from "@/components/EligibilityDetailModal"
import { STATUS_CONFIG } from "@/types/eligibility"

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
  brazil: "Brasil",
  cam: "CAM",
  chile: "Chile",
  colombia: "Colombia",
  ecuador: "Ecuador",
  mexico: "Mexico",
  peru: "Peru",
}

interface EligibilityCountryCardProps {
  countryCode: string
  period: string
  lastFinalizedDate: string | null
}

function periodStringToNumber(period: string): number {
  return parseInt(period, 10)
}

export function EligibilityCountryCard({ countryCode, period, lastFinalizedDate }: EligibilityCountryCardProps) {
  const [modalOpen, setModalOpen] = useState(false)

  const { data: runs, isLoading } = useEligibilityRuns(countryCode, periodStringToNumber(period))
  const createRun = useCreateEligibilityRun()
  const executeRun = useExecuteEligibility()
  const cancelRun = useCancelEligibility()

  const latestRun = runs?.[0] ?? null
  const countryLabel = COUNTRY_LABELS[countryCode] ?? countryCode
  const flag = COUNTRY_FLAGS[countryCode] ?? ""

  const isRunning = latestRun?.status.includes("_running") ?? false
  const isFinalized = latestRun?.status === "finalized"
  const hasActiveProcess = latestRun != null && !["failed", "cancelled", "finalized"].includes(latestRun.status) && latestRun.status !== "pending"
  const canExecute = !isRunning && !createRun.isPending && !executeRun.isPending
  const showDetailButton = latestRun != null && latestRun.status !== "pending"

  const statusConfig = latestRun ? STATUS_CONFIG[latestRun.status] : null

  const handleExecutar = () => {
    createRun.mutate(
      { country: countryCode, period: periodStringToNumber(period) },
      {
        onSuccess: (run) => {
          executeRun.mutate(run.run_id, {
            onSuccess: () => toast.success(`Elegibilidad iniciada para ${countryLabel}`),
            onError: () => toast.error(`Error al ejecutar elegibilidad para ${countryLabel}`),
          })
        },
        onError: () => toast.error(`Error al crear elegibilidad para ${countryLabel}`),
      },
    )
  }

  const handleCancel = () => {
    if (!latestRun) return
    cancelRun.mutate(latestRun.run_id, {
      onSuccess: () => toast.info(`Elegibilidad detenida para ${countryLabel}`),
      onError: () => toast.error(`Error al detener elegibilidad para ${countryLabel}`),
    })
  }

  return (
    <>
      <Card>
        <CardHeader className="pb-3">
          <div className="flex items-center justify-between">
            <CardTitle className="text-base">
              {flag && <span className="mr-1.5">{flag}</span>}
              {countryLabel}
            </CardTitle>
            <Badge variant="outline" className="text-xs" title={lastFinalizedDate ? `Last: ${lastFinalizedDate.replace("T", " ").slice(0, 16)}` : undefined}>
              {lastFinalizedDate
                ? formatRelativeTime(lastFinalizedDate)
                : "never"}
            </Badge>
          </div>
        </CardHeader>
        <CardContent className="space-y-3">
          {/* Latest run status */}
          {isLoading && (
            <div className="h-5 w-24 animate-pulse rounded bg-muted" />
          )}
          {!isLoading && statusConfig && (
            <Badge variant={statusConfig.variant} className={statusConfig.className}>
              {statusConfig.label}
            </Badge>
          )}

          {/* Progress steps when run exists and not pending */}
          {latestRun?.steps && latestRun.steps.length > 0 && latestRun.status !== "pending" && (
            <EligibilityProgress steps={latestRun.steps} currentStep={latestRun.current_step} />
          )}

          {/* Error message */}
          {latestRun?.error_message && (
            <p className="text-xs text-destructive truncate" title={latestRun.error_message}>
              {latestRun.error_message}
            </p>
          )}

          {/* Actions */}
          <div className="flex gap-2">
            {/* Running state: spinner + Detener */}
            {isRunning && (
              <>
                <Button size="sm" disabled className="flex-1">
                  <Loader2 className="mr-2 h-4 w-4 animate-spin" />
                  Ejecutando...
                </Button>
                <AlertDialog>
                  <AlertDialogTrigger asChild>
                    <Button
                      size="sm"
                      variant="destructive"
                      disabled={cancelRun.isPending}
                    >
                      <Square className="mr-2 h-3.5 w-3.5" />
                      Detener
                    </Button>
                  </AlertDialogTrigger>
                  <AlertDialogContent>
                    <AlertDialogHeader>
                      <AlertDialogTitle>Detener ejecucion</AlertDialogTitle>
                      <AlertDialogDescription>
                        Detener la elegibilidad en curso para <strong>{countryLabel}</strong> periodo{" "}
                        <strong>{period}</strong>?
                      </AlertDialogDescription>
                    </AlertDialogHeader>
                    <AlertDialogFooter>
                      <AlertDialogCancel>Volver</AlertDialogCancel>
                      <AlertDialogAction
                        className="bg-destructive text-destructive-foreground hover:bg-destructive/90"
                        onClick={handleCancel}
                      >
                        Detener
                      </AlertDialogAction>
                    </AlertDialogFooter>
                  </AlertDialogContent>
                </AlertDialog>
              </>
            )}

            {/* Ejecutar button: when no active process */}
            {!isRunning && !hasActiveProcess && !isFinalized && (
              <AlertDialog>
                <AlertDialogTrigger asChild>
                  <Button
                    size="sm"
                    disabled={!canExecute}
                    className="flex-1"
                  >
                    {(createRun.isPending || executeRun.isPending) ? (
                      <>
                        <Loader2 className="mr-2 h-4 w-4 animate-spin" />
                        Iniciando...
                      </>
                    ) : (
                      "Ejecutar"
                    )}
                  </Button>
                </AlertDialogTrigger>
                <AlertDialogContent>
                  <AlertDialogHeader>
                    <AlertDialogTitle>Confirmar ejecucion</AlertDialogTitle>
                    <AlertDialogDescription>
                      Iniciar elegibilidad para <strong>{countryLabel}</strong> periodo{" "}
                      <strong>{period}</strong>?
                    </AlertDialogDescription>
                  </AlertDialogHeader>
                  <AlertDialogFooter>
                    <AlertDialogCancel>Cancelar</AlertDialogCancel>
                    <AlertDialogAction onClick={handleExecutar}>Confirmar</AlertDialogAction>
                  </AlertDialogFooter>
                </AlertDialogContent>
              </AlertDialog>
            )}

            {/* Finalized: green checkmark */}
            {isFinalized && (
              <div className="flex items-center gap-1.5 text-green-600 text-sm font-medium flex-1">
                <CheckCircle2 className="h-4 w-4" />
                Finalizado
              </div>
            )}

            {/* Ver detalle button */}
            {showDetailButton && (
              <Button
                size="sm"
                variant="outline"
                onClick={() => setModalOpen(true)}
              >
                <Eye className="mr-2 h-3.5 w-3.5" />
                Ver detalle
              </Button>
            )}
          </div>
        </CardContent>
      </Card>

      <EligibilityDetailModal
        run={latestRun}
        open={modalOpen}
        onOpenChange={setModalOpen}
      />
    </>
  )
}

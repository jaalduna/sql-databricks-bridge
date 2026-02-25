import { useRef, useState } from "react"
import { toast } from "sonner"
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog"
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
import { Button } from "@/components/ui/button"
import { Separator } from "@/components/ui/separator"
import {
  CheckCircle2,
  ChevronDown,
  ChevronRight,
  Circle,
  Download,
  FileText,
  Loader2,
  Upload,
  XCircle,
} from "lucide-react"
import { EligibilityProgress } from "@/components/EligibilityProgress"
import { downloadEligibilityFile } from "@/lib/api"
import {
  useEligibilityRun,
  useExecuteStage2,
  useUploadEligibilityFiles,
  useFinalizeEligibility,
  useCancelEligibility,
} from "@/hooks/useEligibility"
import { STEP_ORDER, STEP_LABELS, STATUS_CONFIG } from "@/types/eligibility"
import type { EligibilityFile, EligibilityRun, EligibilityStep } from "@/types/eligibility"

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

interface EligibilityDetailModalProps {
  run: EligibilityRun | null
  open: boolean
  onOpenChange: (open: boolean) => void
}

function parseUTC(ts: string): number {
  return new Date(ts.endsWith("Z") ? ts : ts + "Z").getTime()
}

function formatDuration(start?: string | null, end?: string | null): string {
  if (!start) return "-"
  const s = parseUTC(start)
  const e = end ? parseUTC(end) : Date.now()
  const totalSeconds = Math.max(0, Math.round((e - s) / 1000))
  if (totalSeconds < 60) return `${totalSeconds}s`
  if (totalSeconds < 3600) {
    const m = Math.floor(totalSeconds / 60)
    const sec = totalSeconds % 60
    return `${m}m ${sec}s`
  }
  const h = Math.floor(totalSeconds / 3600)
  const m = Math.floor((totalSeconds % 3600) / 60)
  const sec = totalSeconds % 60
  return `${h}h ${m}m ${sec}s`
}

function StepIcon({ status }: { status: string }) {
  switch (status) {
    case "completed":
      return <CheckCircle2 className="h-4 w-4 text-green-500 shrink-0" />
    case "running":
      return <Loader2 className="h-4 w-4 text-blue-500 animate-spin shrink-0" />
    case "failed":
      return <XCircle className="h-4 w-4 text-destructive shrink-0" />
    default:
      return <Circle className="h-4 w-4 text-muted-foreground/40 shrink-0" />
  }
}

function formatBytes(bytes: number): string {
  if (bytes < 1024) return `${bytes} B`
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`
  return `${(bytes / (1024 * 1024)).toFixed(1)} MB`
}

interface FileSectionProps {
  title: string
  downloadFiles: EligibilityFile[]
  uploadFiles: EligibilityFile[]
  canUpload: boolean
  runId: string
  onUploadSuccess: () => void
}

function FileSection({ title, downloadFiles, uploadFiles, canUpload, runId, onUploadSuccess }: FileSectionProps) {
  const fileInputRef = useRef<HTMLInputElement>(null)
  const [isDownloading, setIsDownloading] = useState<string | null>(null)
  const uploadMutation = useUploadEligibilityFiles()

  const handleDownload = async (file: EligibilityFile) => {
    setIsDownloading(file.file_id)
    try {
      await downloadEligibilityFile(runId, file.file_id, file.filename)
      toast.success(`${file.filename} descargado`)
    } catch {
      toast.error("Error al descargar archivo")
    } finally {
      setIsDownloading(null)
    }
  }

  const handleFileSelect = (e: React.ChangeEvent<HTMLInputElement>) => {
    const file = e.target.files?.[0]
    if (!file) return
    uploadMutation.mutate(
      { runId, file },
      {
        onSuccess: () => {
          toast.success(`${file.name} subido correctamente`)
          onUploadSuccess()
          if (fileInputRef.current) fileInputRef.current.value = ""
        },
        onError: () => {
          toast.error("Error al subir archivo")
          if (fileInputRef.current) fileInputRef.current.value = ""
        },
      },
    )
  }

  return (
    <div className="space-y-2">
      <span className="text-xs font-medium text-muted-foreground">{title}</span>

      {downloadFiles.length > 0 && (
        <div className="space-y-1">
          {downloadFiles.map((f) => (
            <div
              key={f.file_id}
              className="flex items-center justify-between rounded border bg-muted/30 px-2 py-1.5 text-xs"
            >
              <div className="flex items-center gap-2 min-w-0">
                <FileText className="h-3.5 w-3.5 text-muted-foreground shrink-0" />
                <span className="truncate font-mono">{f.filename}</span>
                {f.file_size_bytes != null && (
                  <span className="text-muted-foreground shrink-0">
                    ({formatBytes(f.file_size_bytes)})
                  </span>
                )}
              </div>
              <Button
                variant="ghost"
                size="sm"
                className="h-7 w-7 p-0 shrink-0"
                disabled={isDownloading === f.file_id}
                onClick={() => handleDownload(f)}
              >
                {isDownloading === f.file_id ? (
                  <Loader2 className="h-3.5 w-3.5 animate-spin" />
                ) : (
                  <Download className="h-3.5 w-3.5" />
                )}
              </Button>
            </div>
          ))}
        </div>
      )}

      {uploadFiles.length > 0 && (
        <div className="space-y-1">
          {uploadFiles.map((f) => (
            <div
              key={f.file_id}
              className="flex items-center gap-2 rounded border border-green-500/30 bg-green-500/5 px-2 py-1.5 text-xs"
            >
              <CheckCircle2 className="h-3.5 w-3.5 text-green-500 shrink-0" />
              <span className="truncate font-mono">{f.filename}</span>
              {f.file_size_bytes != null && (
                <span className="text-muted-foreground">({formatBytes(f.file_size_bytes)})</span>
              )}
            </div>
          ))}
        </div>
      )}

      {canUpload && uploadFiles.length === 0 && (
        <div
          className="flex flex-col items-center justify-center gap-2 rounded-md border-2 border-dashed border-muted-foreground/25 p-4 cursor-pointer hover:border-muted-foreground/50 transition-colors"
          onClick={() => fileInputRef.current?.click()}
          onKeyDown={(e) => { if (e.key === "Enter") fileInputRef.current?.click() }}
          role="button"
          tabIndex={0}
        >
          {uploadMutation.isPending ? (
            <Loader2 className="h-6 w-6 text-muted-foreground animate-spin" />
          ) : (
            <Upload className="h-6 w-6 text-muted-foreground" />
          )}
          <span className="text-xs text-muted-foreground text-center">
            {uploadMutation.isPending
              ? "Subiendo..."
              : "Arrastra un CSV o haz clic para seleccionar"}
          </span>
          <input
            ref={fileInputRef}
            type="file"
            accept=".csv"
            className="hidden"
            onChange={handleFileSelect}
            disabled={uploadMutation.isPending}
          />
        </div>
      )}
    </div>
  )
}

export function EligibilityDetailModal({ run: initialRun, open, onOpenChange }: EligibilityDetailModalProps) {
  const [expandedSteps, setExpandedSteps] = useState<Set<string>>(new Set())

  const { data: liveRun } = useEligibilityRun(open && initialRun ? initialRun.run_id : null)
  const run = liveRun ?? initialRun

  const executeStage2 = useExecuteStage2()
  const finalize = useFinalizeEligibility()
  const cancel = useCancelEligibility()

  if (!run) return null

  const steps = run.steps ?? []
  const files = run.files ?? []
  const statusConfig = STATUS_CONFIG[run.status]
  const countryLabel = COUNTRY_LABELS[run.country] ?? run.country

  const stage1Downloads = files.filter((f) => f.stage === 1 && f.direction === "download")
  const stage1Uploads = files.filter((f) => f.stage === 1 && f.direction === "upload")
  const stage2Downloads = files.filter((f) => f.stage === 2 && f.direction === "download")
  const stage2Uploads = files.filter((f) => f.stage === 2 && f.direction === "upload")

  const isRunning = run.status.includes("_running")
  const canUploadStage1 = run.status === "stage1_ready" && stage1Uploads.length === 0
  const canUploadStage2 = run.status === "stage2_ready" && stage2Uploads.length === 0
  const showStage1Files = ["stage1_ready", "stage1_uploaded", "stage2_running", "stage2_ready", "stage2_uploaded", "finalized"].includes(run.status)
  const showStage2Files = ["stage2_ready", "stage2_uploaded", "finalized"].includes(run.status)

  function toggleStep(stepName: string) {
    setExpandedSteps((prev) => {
      const next = new Set(prev)
      if (next.has(stepName)) {
        next.delete(stepName)
      } else {
        next.add(stepName)
      }
      return next
    })
  }

  const handleExecuteStage2 = () => {
    executeStage2.mutate(run.run_id, {
      onSuccess: () => toast.success("Fase 2 iniciada"),
      onError: () => toast.error("Error al iniciar Fase 2"),
    })
  }

  const handleFinalize = () => {
    finalize.mutate(run.run_id, {
      onSuccess: () => toast.success("Elegibilidad finalizada"),
      onError: () => toast.error("Error al finalizar"),
    })
  }

  const handleCancel = () => {
    cancel.mutate(run.run_id, {
      onSuccess: () => toast.info("Elegibilidad cancelada"),
      onError: () => toast.error("Error al cancelar"),
    })
  }

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="max-w-lg max-h-[85vh] overflow-y-auto">
        <DialogHeader>
          <DialogTitle className="flex items-center gap-2">
            Detalle Elegibilidad
            <Badge variant={statusConfig.variant} className={statusConfig.className}>
              {statusConfig.label}
            </Badge>
          </DialogTitle>
          <DialogDescription>
            Detalles del proceso de elegibilidad, archivos y progreso del pipeline.
          </DialogDescription>
        </DialogHeader>

        <div className="space-y-3 text-sm">
          {/* Metadata grid */}
          <div className="grid grid-cols-2 gap-2">
            <div>
              <span className="text-muted-foreground">Pais</span>
              <p className="font-medium">{countryLabel}</p>
            </div>
            <div>
              <span className="text-muted-foreground">Periodo</span>
              <p className="font-medium font-mono text-xs">{run.period}</p>
            </div>
            <div>
              <span className="text-muted-foreground">Inicio</span>
              <p className="font-medium">
                {run.started_at ? new Date(parseUTC(run.started_at)).toLocaleString() : "-"}
              </p>
            </div>
            <div>
              <span className="text-muted-foreground">Duracion</span>
              <p className="font-medium">
                {formatDuration(run.started_at, run.completed_at)}
              </p>
            </div>
            {run.created_by && (
              <div>
                <span className="text-muted-foreground">Creado por</span>
                <p className="font-medium">{run.created_by}</p>
              </div>
            )}
          </div>

          {/* Pipeline steps */}
          {steps.length > 0 && (
            <>
              <Separator />
              <div>
                <span className="text-muted-foreground">Pipeline</span>
                <div className="mt-2">
                  <EligibilityProgress steps={steps} currentStep={run.current_step} />
                </div>
                <div className="mt-3 space-y-1">
                  {STEP_ORDER.map((stepName) => {
                    const step = steps.find((s) => s.name === stepName)
                    if (!step) return null
                    const isExpanded = expandedSteps.has(stepName)
                    const hasDetails = step.started_at != null

                    return (
                      <StepRow
                        key={stepName}
                        step={step}
                        expanded={isExpanded}
                        onToggle={() => toggleStep(stepName)}
                        hasDetails={hasDetails}
                      >
                        {step.started_at && (
                          <div className="ml-6 mt-1 border-l-2 border-muted pl-2 pb-1">
                            <div className="text-[11px] text-muted-foreground px-2 py-0.5">
                              <span>Inicio: {new Date(parseUTC(step.started_at)).toLocaleTimeString()}</span>
                              {step.completed_at && (
                                <span className="ml-3">
                                  Fin: {new Date(parseUTC(step.completed_at)).toLocaleTimeString()}
                                </span>
                              )}
                            </div>
                          </div>
                        )}
                      </StepRow>
                    )
                  })}
                </div>
              </div>
            </>
          )}

          {/* Stage 1 files */}
          {showStage1Files && (
            <>
              <Separator />
              <FileSection
                title="Fase 1 - Archivos"
                downloadFiles={stage1Downloads}
                uploadFiles={stage1Uploads}
                canUpload={canUploadStage1}
                runId={run.run_id}
                onUploadSuccess={() => {}}
              />
            </>
          )}

          {/* Stage 2 files */}
          {showStage2Files && (
            <>
              <Separator />
              <FileSection
                title="Fase 2 - Archivos"
                downloadFiles={stage2Downloads}
                uploadFiles={stage2Uploads}
                canUpload={canUploadStage2}
                runId={run.run_id}
                onUploadSuccess={() => {}}
              />
            </>
          )}

          {/* Error */}
          {run.error_message && (
            <>
              <Separator />
              <div>
                <span className="text-muted-foreground">Error</span>
                <p className="mt-1 rounded bg-destructive/10 p-2 text-xs text-destructive font-mono">
                  {run.error_message}
                </p>
              </div>
            </>
          )}
        </div>

        {/* Action buttons */}
        <DialogFooter className="gap-2 sm:gap-0">
          {run.status === "stage1_uploaded" && (
            <AlertDialog>
              <AlertDialogTrigger asChild>
                <Button disabled={executeStage2.isPending}>
                  {executeStage2.isPending && <Loader2 className="mr-2 h-4 w-4 animate-spin" />}
                  Ejecutar Fase 2
                </Button>
              </AlertDialogTrigger>
              <AlertDialogContent>
                <AlertDialogHeader>
                  <AlertDialogTitle>Ejecutar Fase 2</AlertDialogTitle>
                  <AlertDialogDescription>
                    Iniciar la Fase 2 de elegibilidad para{" "}
                    <strong>{countryLabel}</strong> periodo <strong>{run.period}</strong>?
                  </AlertDialogDescription>
                </AlertDialogHeader>
                <AlertDialogFooter>
                  <AlertDialogCancel>Cancelar</AlertDialogCancel>
                  <AlertDialogAction onClick={handleExecuteStage2}>Confirmar</AlertDialogAction>
                </AlertDialogFooter>
              </AlertDialogContent>
            </AlertDialog>
          )}

          {run.status === "stage2_uploaded" && (
            <AlertDialog>
              <AlertDialogTrigger asChild>
                <Button className="bg-green-600 hover:bg-green-600/90" disabled={finalize.isPending}>
                  {finalize.isPending && <Loader2 className="mr-2 h-4 w-4 animate-spin" />}
                  Listo
                </Button>
              </AlertDialogTrigger>
              <AlertDialogContent>
                <AlertDialogHeader>
                  <AlertDialogTitle>Finalizar elegibilidad</AlertDialogTitle>
                  <AlertDialogDescription>
                    Finalizar el proceso de elegibilidad para{" "}
                    <strong>{countryLabel}</strong> periodo <strong>{run.period}</strong>?
                  </AlertDialogDescription>
                </AlertDialogHeader>
                <AlertDialogFooter>
                  <AlertDialogCancel>Cancelar</AlertDialogCancel>
                  <AlertDialogAction onClick={handleFinalize}>Finalizar</AlertDialogAction>
                </AlertDialogFooter>
              </AlertDialogContent>
            </AlertDialog>
          )}

          {(isRunning || ["stage1_ready", "stage1_uploaded", "stage2_ready", "stage2_uploaded"].includes(run.status)) && (
            <AlertDialog>
              <AlertDialogTrigger asChild>
                <Button variant="destructive" disabled={cancel.isPending}>
                  {cancel.isPending && <Loader2 className="mr-2 h-4 w-4 animate-spin" />}
                  Cancelar
                </Button>
              </AlertDialogTrigger>
              <AlertDialogContent>
                <AlertDialogHeader>
                  <AlertDialogTitle>Cancelar elegibilidad</AlertDialogTitle>
                  <AlertDialogDescription>
                    Cancelar el proceso de elegibilidad para{" "}
                    <strong>{countryLabel}</strong> periodo <strong>{run.period}</strong>?
                  </AlertDialogDescription>
                </AlertDialogHeader>
                <AlertDialogFooter>
                  <AlertDialogCancel>Volver</AlertDialogCancel>
                  <AlertDialogAction
                    className="bg-destructive text-destructive-foreground hover:bg-destructive/90"
                    onClick={handleCancel}
                  >
                    Cancelar
                  </AlertDialogAction>
                </AlertDialogFooter>
              </AlertDialogContent>
            </AlertDialog>
          )}
        </DialogFooter>
      </DialogContent>
    </Dialog>
  )
}

interface StepRowProps {
  step: EligibilityStep
  expanded: boolean
  onToggle: () => void
  hasDetails: boolean
  children?: React.ReactNode
}

function StepRow({ step, expanded, onToggle, hasDetails, children }: StepRowProps) {
  const label = STEP_LABELS[step.name] ?? step.name
  const duration = formatDuration(step.started_at, step.completed_at)

  return (
    <div>
      <button
        type="button"
        onClick={hasDetails ? onToggle : undefined}
        className={`flex items-center justify-between rounded px-2 py-1.5 text-xs w-full text-left ${
          hasDetails ? "cursor-pointer hover:ring-1 hover:ring-ring/30" : "cursor-default"
        } ${
          step.status === "failed"
            ? "bg-destructive/10"
            : step.status === "running"
              ? "bg-blue-500/10"
              : "bg-muted/50"
        }`}
      >
        <div className="flex items-center gap-2 min-w-0">
          {hasDetails ? (
            expanded ? (
              <ChevronDown className="h-3.5 w-3.5 text-muted-foreground shrink-0" />
            ) : (
              <ChevronRight className="h-3.5 w-3.5 text-muted-foreground shrink-0" />
            )
          ) : (
            <span className="w-3.5 shrink-0" />
          )}
          <StepIcon status={step.status} />
          <span className="font-medium">{label}</span>
        </div>
        <div className="flex items-center gap-3 shrink-0 ml-2">
          {step.status !== "pending" && (
            <span className="text-muted-foreground">{duration}</span>
          )}
          <Badge
            variant={
              step.status === "completed"
                ? "default"
                : step.status === "failed"
                  ? "destructive"
                  : step.status === "running"
                    ? "secondary"
                    : "outline"
            }
            className="text-[10px] px-1.5 py-0"
          >
            {step.status}
          </Badge>
        </div>
      </button>
      {expanded && children}
    </div>
  )
}

import { useEffect, useRef, useState } from "react"
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
import { Input } from "@/components/ui/input"
import { Separator } from "@/components/ui/separator"
import {
  CheckCircle2,
  ChevronDown,
  ChevronRight,
  Circle,
  Database,
  Download,
  FileText,
  Loader2,
  RefreshCcw,
  Upload,
  XCircle,
} from "lucide-react"
import { EligibilityProgress } from "./EligibilityProgress"
import { downloadEligibilityFile } from "@/lib/api"
import {
  useEligibilityRun,
  useApproveEligibility,
  useUploadEligibilityFiles,
  useApplyMordom,
  useCancelEligibility,
} from "../hooks/useEligibility"
import { STEP_ORDER, STEP_LABELS, ACTIVE_STATUSES, STATUS_CONFIG } from "../types"
import type { EligibilityFile, EligibilityRun, EligibilityStep } from "../types"

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
  onRerun?: (params: Record<string, unknown>) => void
}

function parseUTC(ts: string): number {
  return new Date(ts).getTime()
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


function FileList({ files, runId }: { files: EligibilityFile[]; runId: string }) {
  const [isDownloading, setIsDownloading] = useState<string | null>(null)

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

  return (
    <div className="space-y-1">
      {files.map((f) => (
        <div
          key={f.file_id}
          className={`flex items-center justify-between rounded border px-2 py-1.5 text-xs ${
            f.direction === "upload"
              ? "border-green-500/30 bg-green-500/5"
              : "bg-muted/30"
          }`}
        >
          <div className="flex items-center gap-2 min-w-0">
            {f.direction === "upload" ? (
              <CheckCircle2 className="h-3.5 w-3.5 text-green-500 shrink-0" />
            ) : (
              <FileText className="h-3.5 w-3.5 text-muted-foreground shrink-0" />
            )}
            <span className="truncate font-mono">{f.filename}</span>
            {f.file_size_bytes != null && (
              <span className="text-muted-foreground shrink-0">
                ({formatBytes(f.file_size_bytes)})
              </span>
            )}
          </div>
          {f.direction === "download" && (
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
          )}
        </div>
      ))}
    </div>
  )
}


function MordomUploadZone({ runId, onSuccess }: { runId: string; onSuccess: () => void }) {
  const fileInputRef = useRef<HTMLInputElement>(null)
  const uploadMutation = useUploadEligibilityFiles()

  const handleFileSelect = (e: React.ChangeEvent<HTMLInputElement>) => {
    const file = e.target.files?.[0]
    if (!file) return
    uploadMutation.mutate(
      { runId, file },
      {
        onSuccess: () => {
          toast.success(`${file.name} subido correctamente`)
          onSuccess()
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
          : "Arrastra el CSV corregido o haz clic para seleccionar"}
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
  )
}


export function EligibilityDetailModal({ run: initialRun, open, onOpenChange, onRerun }: EligibilityDetailModalProps) {
  const [expandedSteps, setExpandedSteps] = useState<Set<string>>(new Set())
  const [editableParams, setEditableParams] = useState<Record<string, unknown>>({})

  const { data: liveRun } = useEligibilityRun(open && initialRun ? initialRun.run_id : null)
  const run = liveRun ?? initialRun

  const approve = useApproveEligibility()
  const applyMordomMutation = useApplyMordom()
  const cancel = useCancelEligibility()

  // Initialize editable params from run's parameters when modal opens or run changes
  useEffect(() => {
    if (run?.parameters_json && Object.keys(run.parameters_json).length > 0) {
      setEditableParams({ ...run.parameters_json })
    }
  }, [run?.run_id, open])

  if (!run) return null

  const canRerun = run.status === "results_ready" && !!onRerun

  const steps = run.steps ?? []
  const files = run.files ?? []
  const statusConfig = STATUS_CONFIG[run.status]
  const countryLabel = COUNTRY_LABELS[run.country] ?? run.country

  // File categories
  const resultFiles = files.filter((f) => f.stage === 1 && f.direction === "download")
  const mordomDownloads = files.filter((f) => f.stage === 2 && f.direction === "download")
  const mordomUploads = files.filter((f) => f.stage === 2 && f.direction === "upload")

  const isProcessing = ACTIVE_STATUSES.includes(run.status)
  const canCancel = !["ready", "failed", "cancelled"].includes(run.status) && run.status !== "pending"

  function toggleStep(stepName: string) {
    setExpandedSteps((prev) => {
      const next = new Set(prev)
      if (next.has(stepName)) next.delete(stepName)
      else next.add(stepName)
      return next
    })
  }

  const handleApprove = () => {
    approve.mutate(run.run_id, {
      onSuccess: () => toast.success("Resultados aprobados — aplicando a SQL Server..."),
      onError: () => toast.error("Error al aprobar"),
    })
  }

  const handleApplyMordom = () => {
    applyMordomMutation.mutate(run.run_id, {
      onSuccess: () => toast.success("Correcciones MorDom aplicadas"),
      onError: () => toast.error("Error al aplicar correcciones MorDom"),
    })
  }

  const handleCancel = () => {
    cancel.mutate(run.run_id, {
      onSuccess: () => toast.info("Ejecución cancelada"),
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
            Detalle del proceso de elegibilidad, archivos y progreso.
          </DialogDescription>
        </DialogHeader>

        <div className="space-y-3 text-sm">
          {/* Metadata grid */}
          <div className="grid grid-cols-2 gap-2">
            <div>
              <span className="text-muted-foreground">País</span>
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
              <span className="text-muted-foreground">Duración</span>
              <p className="font-medium">
                {formatDuration(run.started_at, run.completed_at)}
              </p>
            </div>
            {run.approved_by && (
              <div>
                <span className="text-muted-foreground">Aprobado por</span>
                <p className="font-medium">{run.approved_by}</p>
              </div>
            )}
          </div>

          {/* Parameters — editable when results_ready, read-only otherwise */}
          {run.parameters_json && Object.keys(run.parameters_json).length > 0 && (
            <>
              <Separator />
              <div>
                <span className="text-muted-foreground flex items-center gap-1.5">
                  Parámetros
                  {canRerun && (
                    <span className="text-[10px] text-amber-500 font-normal">(editables para re-ejecución)</span>
                  )}
                </span>
                <div className="mt-1 grid grid-cols-2 gap-x-4 gap-y-1">
                  {Object.entries(canRerun ? editableParams : run.parameters_json).map(([key, value]) => (
                    <div key={key} className="flex items-center justify-between text-xs gap-2">
                      <span className="text-muted-foreground font-mono truncate">{key}</span>
                      {canRerun ? (
                        <Input
                          type="number"
                          step="any"
                          className="h-6 w-20 text-xs font-mono text-right px-1.5"
                          value={String(value ?? "")}
                          onChange={(e) =>
                            setEditableParams((prev) => ({
                              ...prev,
                              [key]: e.target.value ? parseFloat(e.target.value) : 0,
                            }))
                          }
                        />
                      ) : (
                        <span className="font-medium font-mono">{String(value)}</span>
                      )}
                    </div>
                  ))}
                </div>
              </div>
            </>
          )}

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

          {/* ── Phase A: Results files (download to review) ── */}
          {resultFiles.length > 0 && (
            <>
              <Separator />
              <div>
                <span className="text-muted-foreground flex items-center gap-1.5">
                  <Download className="h-3.5 w-3.5" />
                  Resultados — descarga para revisar
                </span>
                <div className="mt-2">
                  <FileList files={resultFiles} runId={run.run_id} />
                </div>
                {run.status === "results_ready" && (
                  <p className="mt-2 text-xs text-muted-foreground italic">
                    Descarga y revisa los resultados. Si estás conforme, aprueba. Si no, cambia parámetros y re-ejecuta.
                  </p>
                )}
              </div>
            </>
          )}

          {/* ── Phase C: MorDom files ── */}
          {(mordomDownloads.length > 0 || mordomUploads.length > 0 || run.status === "mordom_ready") && (
            <>
              <Separator />
              <div>
                <span className="text-muted-foreground flex items-center gap-1.5">
                  <Database className="h-3.5 w-3.5" />
                  MorDom — validación de hogares
                </span>

                {mordomDownloads.length > 0 && (
                  <div className="mt-2">
                    <span className="text-xs text-muted-foreground">Descarga MorDom (hogares nuevos)</span>
                    <FileList files={mordomDownloads} runId={run.run_id} />
                  </div>
                )}

                {mordomUploads.length > 0 && (
                  <div className="mt-2">
                    <span className="text-xs text-muted-foreground">MorDom corregido (subido)</span>
                    <FileList files={mordomUploads} runId={run.run_id} />
                  </div>
                )}

                {run.status === "mordom_ready" && mordomUploads.length === 0 && (
                  <div className="mt-2">
                    <p className="text-xs text-muted-foreground mb-2">
                      Descarga el CSV, revisa que los hogares nuevos tengan todos sus atributos. Marca con "remove" los hogares incompletos y sube el CSV corregido.
                    </p>
                    <MordomUploadZone runId={run.run_id} onSuccess={() => {}} />
                  </div>
                )}
              </div>
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

        {/* ── Action buttons ── */}
        <DialogFooter className="gap-2 sm:gap-0">
          {/* Re-run: when results are ready and user wants different params */}
          {canRerun && (
            <AlertDialog>
              <AlertDialogTrigger asChild>
                <Button variant="outline">
                  <RefreshCcw className="mr-2 h-4 w-4" />
                  Re-ejecutar
                </Button>
              </AlertDialogTrigger>
              <AlertDialogContent>
                <AlertDialogHeader>
                  <AlertDialogTitle>Re-ejecutar pipeline</AlertDialogTitle>
                  <AlertDialogDescription>
                    Crear una nueva ejecución para{" "}
                    <strong>{countryLabel}</strong> periodo <strong>{run.period}</strong> con los parámetros modificados?
                  </AlertDialogDescription>
                </AlertDialogHeader>
                <AlertDialogFooter>
                  <AlertDialogCancel>Cancelar</AlertDialogCancel>
                  <AlertDialogAction
                    onClick={() => {
                      onRerun!(editableParams)
                      onOpenChange(false)
                    }}
                  >
                    Re-ejecutar
                  </AlertDialogAction>
                </AlertDialogFooter>
              </AlertDialogContent>
            </AlertDialog>
          )}

          {/* Approve: when results are ready */}
          {run.status === "results_ready" && (
            <AlertDialog>
              <AlertDialogTrigger asChild>
                <Button className="bg-green-600 hover:bg-green-600/90" disabled={approve.isPending}>
                  {approve.isPending && <Loader2 className="mr-2 h-4 w-4 animate-spin" />}
                  Aprobar y Aplicar
                </Button>
              </AlertDialogTrigger>
              <AlertDialogContent>
                <AlertDialogHeader>
                  <AlertDialogTitle>Aprobar resultados</AlertDialogTitle>
                  <AlertDialogDescription>
                    Aprobar los resultados de elegibilidad para{" "}
                    <strong>{countryLabel}</strong> periodo <strong>{run.period}</strong>?
                    Esto escribirá los resultados en SQL Server y descargará MorDom actualizado.
                  </AlertDialogDescription>
                </AlertDialogHeader>
                <AlertDialogFooter>
                  <AlertDialogCancel>Cancelar</AlertDialogCancel>
                  <AlertDialogAction onClick={handleApprove}>Aprobar</AlertDialogAction>
                </AlertDialogFooter>
              </AlertDialogContent>
            </AlertDialog>
          )}

          {/* Apply MorDom: when corrected CSV uploaded */}
          {run.status === "mordom_uploaded" && (
            <AlertDialog>
              <AlertDialogTrigger asChild>
                <Button className="bg-green-600 hover:bg-green-600/90" disabled={applyMordomMutation.isPending}>
                  {applyMordomMutation.isPending && <Loader2 className="mr-2 h-4 w-4 animate-spin" />}
                  Aplicar MorDom y Finalizar
                </Button>
              </AlertDialogTrigger>
              <AlertDialogContent>
                <AlertDialogHeader>
                  <AlertDialogTitle>Aplicar correcciones MorDom</AlertDialogTitle>
                  <AlertDialogDescription>
                    Aplicar las correcciones de MorDom para{" "}
                    <strong>{countryLabel}</strong> periodo <strong>{run.period}</strong>?
                    Esto marcará la elegibilidad como lista para este país.
                  </AlertDialogDescription>
                </AlertDialogHeader>
                <AlertDialogFooter>
                  <AlertDialogCancel>Cancelar</AlertDialogCancel>
                  <AlertDialogAction onClick={handleApplyMordom}>Aplicar y Finalizar</AlertDialogAction>
                </AlertDialogFooter>
              </AlertDialogContent>
            </AlertDialog>
          )}

          {/* Processing indicator */}
          {isProcessing && (
            <Button disabled>
              <Loader2 className="mr-2 h-4 w-4 animate-spin" />
              Procesando...
            </Button>
          )}

          {/* Cancel: any active state */}
          {canCancel && (
            <AlertDialog>
              <AlertDialogTrigger asChild>
                <Button variant="destructive" disabled={cancel.isPending}>
                  {cancel.isPending && <Loader2 className="mr-2 h-4 w-4 animate-spin" />}
                  Cancelar
                </Button>
              </AlertDialogTrigger>
              <AlertDialogContent>
                <AlertDialogHeader>
                  <AlertDialogTitle>Cancelar proceso</AlertDialogTitle>
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
  const label = STEP_LABELS[step.name as keyof typeof STEP_LABELS] ?? step.name
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

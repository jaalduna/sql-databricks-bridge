import { Progress } from "@/components/ui/progress"
import { CheckCircle2, Circle, Loader2, XCircle } from "lucide-react"
import type { EventDetail } from "@/types/api"
import { CALIBRATION_STEPS, CALIBRATION_STEP_LABELS } from "@/types/api"

interface CalibrationProgressProps {
  job: EventDetail
}

export function CalibrationProgress({ job }: CalibrationProgressProps) {
  const steps = job.steps
  const hasSteps = steps && steps.length > 0

  // Compute step-based progress if steps are available
  // sync_data step counts as fractional progress based on query completion
  let percent: number
  let label: string

  if (hasSteps) {
    const total = steps.length
    let progress = 0
    for (const s of steps) {
      if (s.status === "completed") {
        progress += 1
      } else if (s.status === "running" && s.name === "sync_data" && job.queries_total > 0) {
        // sync_data sub-progress based on queries
        progress += job.queries_completed / job.queries_total
      }
    }
    percent = total > 0 ? Math.round((progress / total) * 100) : 0
    const currentStep = job.current_step
    if (currentStep === "sync_data" && job.queries_total > 0) {
      label = `Sync Data (${job.queries_completed}/${job.queries_total} queries)`
    } else if (currentStep) {
      // Show running Databricks task name if available
      const currentStepObj = steps.find((s) => s.name === currentStep)
      const runningTask = currentStepObj?.tasks?.find((t) => t.status === "running")
      if (runningTask) {
        label = `${CALIBRATION_STEP_LABELS[currentStep] ?? currentStep} — ${runningTask.task_key}`
      } else {
        label = CALIBRATION_STEP_LABELS[currentStep] ?? currentStep
      }
    } else {
      const completed = steps.filter((s) => s.status === "completed").length
      label = `${completed}/${total} steps`
    }
  } else {
    // Fallback to query-based progress
    percent = job.queries_total > 0 ? Math.round((job.queries_completed / job.queries_total) * 100) : 0
    label = `${job.queries_completed}/${job.queries_total} queries`
  }

  return (
    <div className="space-y-2">
      <div className="flex items-center justify-between text-xs text-muted-foreground">
        <span>{label}</span>
        <span>{percent}%</span>
      </div>
      <Progress value={percent} className="h-2" />

      {/* Step indicators */}
      {hasSteps && (
        <div className="flex items-center gap-0.5 pt-1">
          {CALIBRATION_STEPS.map((stepName) => {
            const step = steps.find((s) => s.name === stepName)
            const status = step?.status ?? "pending"
            return (
              <StepDot key={stepName} status={status} label={CALIBRATION_STEP_LABELS[stepName]} />
            )
          })}
        </div>
      )}
    </div>
  )
}

function StepDot({ status, label }: { status: string; label: string }) {
  return (
    <div className="flex flex-col items-center flex-1 min-w-0" title={label}>
      {status === "completed" && (
        <CheckCircle2 className="h-3.5 w-3.5 text-green-500" />
      )}
      {status === "running" && (
        <Loader2 className="h-3.5 w-3.5 text-blue-500 animate-spin" />
      )}
      {status === "failed" && (
        <XCircle className="h-3.5 w-3.5 text-destructive" />
      )}
      {status === "pending" && (
        <Circle className="h-3.5 w-3.5 text-muted-foreground/40" />
      )}
    </div>
  )
}

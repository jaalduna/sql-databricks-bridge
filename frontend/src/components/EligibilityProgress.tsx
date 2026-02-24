import { Progress } from "@/components/ui/progress"
import { CheckCircle2, Circle, Loader2, XCircle } from "lucide-react"
import { STEP_ORDER, STEP_LABELS } from "@/types/eligibility"
import type { EligibilityStep } from "@/types/eligibility"

interface EligibilityProgressProps {
  steps: EligibilityStep[]
  currentStep?: string
}

export function EligibilityProgress({ steps, currentStep }: EligibilityProgressProps) {
  const total = STEP_ORDER.length
  let completed = 0
  for (const s of steps) {
    if (s.status === "completed") {
      completed += 1
    }
  }
  const percent = total > 0 ? Math.round((completed / total) * 100) : 0

  const label = currentStep
    ? (STEP_LABELS[currentStep as keyof typeof STEP_LABELS] ?? currentStep)
    : `${completed}/${total} pasos`

  return (
    <div className="space-y-2">
      <div className="flex items-center justify-between text-xs text-muted-foreground">
        <span>{label}</span>
        <span>{percent}%</span>
      </div>
      <Progress value={percent} className="h-2" />

      <div className="flex items-center gap-0.5 pt-1">
        {STEP_ORDER.map((stepName) => {
          const step = steps.find((s) => s.name === stepName)
          const status = step?.status ?? "pending"
          return (
            <StepDot key={stepName} status={status} label={STEP_LABELS[stepName]} />
          )
        })}
      </div>
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

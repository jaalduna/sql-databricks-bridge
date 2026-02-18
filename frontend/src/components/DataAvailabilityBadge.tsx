import { CheckCircle2, XCircle } from "lucide-react"

interface DataAvailabilityBadgeProps {
  label: string
  available: boolean
}

export function DataAvailabilityBadge({ label, available }: DataAvailabilityBadgeProps) {
  return (
    <div className="flex items-center gap-1.5 text-sm">
      {available ? (
        <CheckCircle2 className="h-4 w-4 text-green-600" />
      ) : (
        <XCircle className="h-4 w-4 text-red-500" />
      )}
      <span className={available ? "text-foreground" : "text-muted-foreground"}>
        {label}
      </span>
    </div>
  )
}

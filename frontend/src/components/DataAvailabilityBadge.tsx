import { CheckCircle2, XCircle, Loader2 } from "lucide-react"

interface DataAvailabilityBadgeProps {
  label: string
  available: boolean
  loading?: boolean
}

export function DataAvailabilityBadge({ label, available, loading }: DataAvailabilityBadgeProps) {
  return (
    <div className="flex items-center gap-1.5 text-sm">
      {loading ? (
        <Loader2 className="h-4 w-4 animate-spin text-muted-foreground" />
      ) : available ? (
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

import { Badge } from "@/components/ui/badge"
import type { JobStatus } from "@/types/api"

const statusConfig: Record<JobStatus, { label: string; variant: "default" | "secondary" | "destructive" | "outline"; className: string }> = {
  pending: { label: "Pending", variant: "outline", className: "" },
  running: { label: "Running", variant: "default", className: "bg-blue-500 hover:bg-blue-500/80" },
  completed: { label: "Completed", variant: "default", className: "bg-green-600 hover:bg-green-600/80" },
  failed: { label: "Failed", variant: "destructive", className: "" },
  cancelled: { label: "Cancelled", variant: "secondary", className: "" },
}

interface StatusBadgeProps {
  status: JobStatus
  className?: string
}

export function StatusBadge({ status, className }: StatusBadgeProps) {
  const config = statusConfig[status]
  return (
    <Badge variant={config.variant} className={`${config.className} ${className ?? ""}`}>
      {config.label}
    </Badge>
  )
}

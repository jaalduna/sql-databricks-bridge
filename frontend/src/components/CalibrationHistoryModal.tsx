import { useState } from "react"
import { toast } from "sonner"
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog"
import { Badge } from "@/components/ui/badge"
import { Button } from "@/components/ui/button"
import { Separator } from "@/components/ui/separator"
import { Skeleton } from "@/components/ui/skeleton"
import { Download, Loader2, FileText, Clock, Calendar } from "lucide-react"
import { downloadCSV } from "@/lib/api"
import { useCalibrationHistory } from "@/hooks/useCalibrationHistory"
import type { EventSummary, JobStatus } from "@/types/api"

interface CalibrationHistoryModalProps {
  country: string
  countryLabel: string
  countryFlag: string
  open: boolean
  onClose: () => void
}

function parseUTC(ts: string): number {
  return new Date(ts.endsWith("Z") ? ts : ts + "Z").getTime()
}

function formatDate(isoDate: string): string {
  const d = new Date(parseUTC(isoDate))
  return d.toLocaleDateString("en-US", {
    month: "short",
    day: "numeric",
    year: "numeric",
  }) + " " + d.toLocaleTimeString("en-US", {
    hour: "2-digit",
    minute: "2-digit",
    hour12: false,
  })
}

function formatDuration(start: string | null, end: string | null): string {
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
  return `${h}h ${m}m`
}

interface StatusBadgeConfig {
  className: string
  label: string
}

function getStatusConfig(status: JobStatus): StatusBadgeConfig {
  switch (status) {
    case "completed":
      return { className: "bg-green-100 text-green-800 border-green-200", label: "Completed" }
    case "failed":
      return { className: "bg-red-100 text-red-800 border-red-200", label: "Failed" }
    case "cancelled":
      return { className: "bg-gray-100 text-gray-700 border-gray-200", label: "Cancelled" }
    case "running":
      return { className: "bg-blue-100 text-blue-800 border-blue-200", label: "Running" }
    case "pending":
      return { className: "bg-yellow-100 text-yellow-800 border-yellow-200", label: "Pending" }
    default:
      return { className: "bg-gray-100 text-gray-700 border-gray-200", label: status }
  }
}

function HistoryEntrySkeleton() {
  return (
    <div className="flex items-center justify-between py-3 px-1">
      <div className="space-y-2 flex-1 min-w-0">
        <Skeleton className="h-4 w-40" />
        <Skeleton className="h-3 w-28" />
        <Skeleton className="h-3 w-24" />
      </div>
      <div className="flex items-center gap-3 shrink-0 ml-4">
        <Skeleton className="h-5 w-20 rounded-full" />
        <Skeleton className="h-3 w-14" />
        <Skeleton className="h-8 w-8 rounded" />
      </div>
    </div>
  )
}

interface HistoryEntryProps {
  event: EventSummary
  isDownloading: boolean
  onDownload: (jobId: string) => void
}

function HistoryEntry({ event, isDownloading, onDownload }: HistoryEntryProps) {
  const statusConfig = getStatusConfig(event.status)
  const isCompleted = event.status === "completed"
  const dateStr = formatDate(event.created_at)
  const duration = formatDuration(event.started_at, event.completed_at)

  return (
    <div className="flex items-center justify-between py-3 px-1 group">
      {/* Left: date, period, rows */}
      <div className="min-w-0 flex-1 space-y-1">
        <div className="flex items-center gap-1.5 text-sm font-medium text-foreground">
          <Calendar className="h-3.5 w-3.5 text-muted-foreground shrink-0" />
          <span>{dateStr}</span>
        </div>
        {event.period && (
          <div className="flex items-center gap-1.5 text-xs text-muted-foreground">
            <FileText className="h-3 w-3 shrink-0" />
            <span>Period: <span className="font-mono font-medium text-foreground">{event.period}</span></span>
          </div>
        )}
        <div className="flex items-center gap-1.5 text-xs text-muted-foreground">
          <span>{event.total_rows_extracted.toLocaleString()} rows extracted</span>
        </div>
      </div>

      {/* Right: status badge, duration, download */}
      <div className="flex items-center gap-3 shrink-0 ml-4">
        <Badge
          variant="outline"
          className={`text-xs px-2 py-0.5 ${statusConfig.className}`}
        >
          {statusConfig.label}
        </Badge>

        <div className="flex items-center gap-1 text-xs text-muted-foreground min-w-[50px] justify-end">
          <Clock className="h-3 w-3 shrink-0" />
          <span>{duration}</span>
        </div>

        <Button
          size="sm"
          variant={isCompleted ? "default" : "ghost"}
          className={`h-8 w-8 p-0 ${!isCompleted ? "opacity-30 cursor-not-allowed" : ""}`}
          disabled={!isCompleted || isDownloading}
          aria-label={`Download CSV for ${dateStr}`}
          onClick={() => onDownload(event.job_id)}
          title={isCompleted ? "Download CSV" : "Download only available for completed calibrations"}
        >
          {isDownloading ? (
            <Loader2 className="h-3.5 w-3.5 animate-spin" />
          ) : (
            <Download className="h-3.5 w-3.5" />
          )}
        </Button>
      </div>
    </div>
  )
}

export function CalibrationHistoryModal({
  country,
  countryLabel,
  countryFlag,
  open,
  onClose,
}: CalibrationHistoryModalProps) {
  const [downloadingIds, setDownloadingIds] = useState<Set<string>>(new Set())
  const { data, isLoading, isError } = useCalibrationHistory(country, open)

  async function handleDownload(jobId: string) {
    setDownloadingIds((prev) => new Set(prev).add(jobId))
    try {
      await downloadCSV(jobId)
      toast.success("CSV downloaded successfully")
    } catch {
      toast.error("Failed to download CSV")
    } finally {
      setDownloadingIds((prev) => {
        const next = new Set(prev)
        next.delete(jobId)
        return next
      })
    }
  }

  const items = data?.items ?? []

  return (
    <Dialog open={open} onOpenChange={(v) => !v && onClose()}>
      <DialogContent className="max-w-2xl max-h-[85vh] overflow-y-auto">
        <DialogHeader>
          <DialogTitle className="flex items-center gap-2 text-lg">
            {countryFlag && <span>{countryFlag}</span>}
            <span>{countryLabel}</span>
            <span className="text-muted-foreground font-normal">— Calibration History</span>
          </DialogTitle>
          <DialogDescription>
            Past calibration runs for {countryLabel}. Download CSV files for completed calibrations.
          </DialogDescription>
        </DialogHeader>

        <div className="mt-2">
          {isLoading && (
            <div className="divide-y divide-border">
              {Array.from({ length: 4 }).map((_, i) => (
                <HistoryEntrySkeleton key={i} />
              ))}
            </div>
          )}

          {!isLoading && isError && (
            <div className="flex flex-col items-center justify-center py-12 text-center">
              <FileText className="h-10 w-10 text-destructive/40 mb-3" />
              <p className="text-sm font-medium text-destructive">Failed to load calibration history</p>
              <p className="text-xs text-muted-foreground mt-1">
                Please try again later or check your connection.
              </p>
            </div>
          )}

          {!isLoading && !isError && items.length === 0 && (
            <div className="flex flex-col items-center justify-center py-12 text-center">
              <FileText className="h-10 w-10 text-muted-foreground/40 mb-3" />
              <p className="text-sm font-medium text-muted-foreground">No calibrations found for this country</p>
              <p className="text-xs text-muted-foreground/70 mt-1">
                Calibration history will appear here once runs have been triggered.
              </p>
            </div>
          )}

          {!isLoading && !isError && items.length > 0 && (
            <>
              <div className="text-xs text-muted-foreground mb-3">
                Showing {items.length} most recent calibration{items.length !== 1 ? "s" : ""}
              </div>
              <div className="divide-y divide-border rounded-lg border">
                {items.map((event, idx) => (
                  <div key={event.job_id} className={idx % 2 === 0 ? "bg-background" : "bg-muted/40"}>
                    <div className="px-3">
                      <HistoryEntry
                        event={event}
                        isDownloading={downloadingIds.has(event.job_id)}
                        onDownload={handleDownload}
                      />
                    </div>
                  </div>
                ))}
              </div>

              {data && data.total > items.length && (
                <>
                  <Separator className="mt-4" />
                  <p className="text-xs text-muted-foreground text-center mt-3">
                    Showing {items.length} of {data.total} total calibrations
                  </p>
                </>
              )}
            </>
          )}
        </div>
      </DialogContent>
    </Dialog>
  )
}

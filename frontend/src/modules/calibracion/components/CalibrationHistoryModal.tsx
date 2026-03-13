import { useState, useEffect } from "react"
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
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select"
import { Separator } from "@/components/ui/separator"
import { Skeleton } from "@/components/ui/skeleton"
import { Download, Loader2, FileText, Clock, Calendar } from "lucide-react"
import { downloadCSV } from "@/lib/api"
import { useCalibrationHistory } from "../hooks/useCalibrationHistory"
import { useJobTables } from "../hooks/useJobTables"
import type { EventSummary, JobStatus, TableInfo } from "@/types/api"

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

interface TablePickerProps {
  jobId: string
  onDownload: (jobId: string, table: string) => Promise<void>
  onCollapse: () => void
}

function TablePicker({ jobId, onDownload, onCollapse }: TablePickerProps) {
  const { data, isLoading, isError } = useJobTables(jobId)
  const [selectedTable, setSelectedTable] = useState<string>("")
  const [isDownloading, setIsDownloading] = useState(false)

  const tables: TableInfo[] = data?.tables ?? []

  // Auto-download when exactly 1 table, or show error when 0 tables
  useEffect(() => {
    if (isLoading || isError) return
    if (tables.length === 0) {
      toast.error("No tables available")
      onCollapse()
    } else if (tables.length === 1) {
      onDownload(jobId, tables[0].full_name).finally(onCollapse)
    }
  }, [isLoading, isError, tables.length]) // eslint-disable-line react-hooks/exhaustive-deps

  // For 0 or 1 tables, the useEffect handles it -- don't render the picker
  if (isLoading) {
    return (
      <div className="flex items-center gap-2 px-4 py-2.5 bg-muted/60 text-xs text-muted-foreground">
        <Loader2 className="h-3.5 w-3.5 animate-spin" />
        <span>Loading available tables...</span>
      </div>
    )
  }

  if (isError) {
    return (
      <div className="flex items-center justify-between px-4 py-2.5 bg-muted/60 text-xs">
        <span className="text-destructive">Failed to load tables</span>
        <button
          onClick={onCollapse}
          className="text-muted-foreground hover:text-foreground underline"
        >
          Cancel
        </button>
      </div>
    )
  }

  // 0 or 1 table: useEffect handles these, render nothing while it runs
  if (tables.length <= 1) return null

  async function handlePickerDownload() {
    if (!selectedTable) return
    setIsDownloading(true)
    try {
      await onDownload(jobId, selectedTable)
    } finally {
      setIsDownloading(false)
      onCollapse()
    }
  }

  return (
    <div className="flex items-center gap-2 px-4 py-2.5 bg-muted/60">
      <span className="text-xs text-muted-foreground shrink-0">Table:</span>
      <Select value={selectedTable} onValueChange={setSelectedTable}>
        <SelectTrigger size="sm" className="h-7 text-xs min-w-[200px]">
          <SelectValue placeholder="Select a table..." />
        </SelectTrigger>
        <SelectContent>
          {tables.map((t) => (
            <SelectItem key={t.full_name} value={t.full_name} className="text-xs">
              {t.schema_name}.{t.table_name}
            </SelectItem>
          ))}
        </SelectContent>
      </Select>
      <Button
        size="sm"
        className="h-7 text-xs px-3"
        disabled={!selectedTable || isDownloading}
        onClick={handlePickerDownload}
      >
        {isDownloading ? (
          <Loader2 className="h-3 w-3 animate-spin mr-1" />
        ) : (
          <Download className="h-3 w-3 mr-1" />
        )}
        Download
      </Button>
      <button
        onClick={onCollapse}
        className="text-xs text-muted-foreground hover:text-foreground ml-auto"
      >
        Cancel
      </button>
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
  const [expandedJobId, setExpandedJobId] = useState<string | null>(null)
  const { data, isLoading, isError } = useCalibrationHistory(country, open)

  function handleDownload(jobId: string) {
    // Toggle: second click on same row collapses the picker
    setExpandedJobId((prev) => (prev === jobId ? null : jobId))
  }

  async function actualDownload(jobId: string, table: string) {
    setDownloadingIds((prev) => new Set(prev).add(jobId))
    try {
      await downloadCSV(jobId, table)
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
                    {expandedJobId === event.job_id && (
                      <TablePicker
                        jobId={event.job_id}
                        onDownload={actualDownload}
                        onCollapse={() => setExpandedJobId(null)}
                      />
                    )}
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

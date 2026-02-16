import { useEffect, useMemo, useState } from "react"
import { Link, useParams, useNavigate } from "react-router-dom"
import { useQuery } from "@tanstack/react-query"
import type { ColumnDef } from "@tanstack/react-table"
import { toast } from "sonner"
import { getEvent, triggerSync } from "@/lib/api"
import type { JobStatus, QueryResult } from "@/types/api"
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
  DialogDescription,
  DialogFooter,
} from "@/components/ui/dialog"
import { Button } from "@/components/ui/button"
import { Progress } from "@/components/ui/progress"
import { Separator } from "@/components/ui/separator"
import { Skeleton } from "@/components/ui/skeleton"
import { StatusBadge } from "@/components/StatusBadge"
import { DataTable } from "@/components/DataTable"
import { Loader2, CheckCircle2, XCircle, Clock, Database, FileText, AlertTriangle, RotateCcw, Layers } from "lucide-react"

function formatRows(n: number): string {
  if (n >= 1_000_000) return `${(n / 1_000_000).toFixed(1)}M`
  if (n >= 1_000) return `${(n / 1_000).toFixed(0)}K`
  return n.toLocaleString()
}

function formatDuration(seconds: number): string {
  if (seconds < 0) seconds = 0
  if (seconds < 1) return `${(seconds * 1000).toFixed(0)}ms`
  if (seconds < 60) return `${seconds.toFixed(1)}s`
  const h = Math.floor(seconds / 3600)
  const m = Math.floor((seconds % 3600) / 60)
  const s = Math.round(seconds % 60)
  if (h > 0) return `${h}h ${String(m).padStart(2, "0")}m ${String(s).padStart(2, "0")}s`
  return `${m}m ${String(s).padStart(2, "0")}s`
}

/** Parse a timestamp string as UTC (backend sends naive UTC without 'Z'). */
function parseUTC(dateStr: string): Date {
  // Append 'Z' if no timezone indicator present so JS parses as UTC
  if (!/[Z+\-]\d{0,4}$/.test(dateStr)) return new Date(dateStr + "Z")
  return new Date(dateStr)
}

function formatTimestamp(dateStr: string | null): string {
  if (!dateStr) return "--"
  return parseUTC(dateStr).toLocaleString("en-US", {
    year: "numeric",
    month: "short",
    day: "numeric",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
  })
}

function computeDuration(started: string | null, completed: string | null): string {
  if (!started) return "--"
  const start = parseUTC(started).getTime()
  const end = completed ? parseUTC(completed).getTime() : Date.now()
  const seconds = (end - start) / 1000
  return formatDuration(seconds)
}

/** Compute a live elapsed duration from a started_at timestamp to now. */
function liveDuration(startedAt: string | null): string {
  if (!startedAt) return "--"
  const start = parseUTC(startedAt).getTime()
  const seconds = (Date.now() - start) / 1000
  return formatDuration(seconds)
}

const queryResultColumns: ColumnDef<QueryResult, unknown>[] = [
  {
    accessorKey: "query_name",
    header: "Query Name",
    cell: ({ getValue, row }) => {
      const isRunning = row.original.status === "running"
      return (
        <span className="font-mono text-sm flex items-center gap-2">
          {isRunning && <Loader2 className="h-3 w-3 animate-spin text-blue-500 shrink-0" />}
          {getValue<string>()}
        </span>
      )
    },
  },
  {
    accessorKey: "status",
    header: "Status",
    cell: ({ getValue }) => <StatusBadge status={getValue<JobStatus>()} />,
    enableSorting: false,
  },
  {
    accessorKey: "rows_extracted",
    header: () => <span className="flex justify-end">Rows</span>,
    cell: ({ getValue }) => {
      const rows = getValue<number>()
      return (
        <span className="flex justify-end font-mono text-sm">
          {rows > 0 ? formatRows(rows) : "--"}
        </span>
      )
    },
  },
  {
    id: "throughput",
    header: () => <span className="flex justify-end">Throughput</span>,
    cell: ({ row }) => {
      const { rows_extracted, duration_seconds } = row.original
      if (rows_extracted > 0 && duration_seconds > 0) {
        return (
          <span className="flex justify-end font-mono text-sm text-muted-foreground">
            {formatRows(Math.round(rows_extracted / duration_seconds))}/s
          </span>
        )
      }
      return <span className="flex justify-end text-muted-foreground">--</span>
    },
  },
  {
    accessorKey: "duration_seconds",
    header: () => <span className="flex justify-end">Duration</span>,
    cell: ({ getValue, row }) => {
      const seconds = getValue<number>()
      // Show live duration for running queries
      if (row.original.status === "running" && row.original.started_at) {
        return (
          <span className="flex justify-end text-blue-500 font-mono text-sm">
            {liveDuration(row.original.started_at)}
          </span>
        )
      }
      return (
        <span className="flex justify-end text-muted-foreground">
          {seconds > 0 ? formatDuration(seconds) : "--"}
        </span>
      )
    },
  },
]

function QueryDetailDialog({
  query,
  open,
  onClose,
}: {
  query: QueryResult | null
  open: boolean
  onClose: () => void
}) {
  if (!query) return null

  const isFailed = query.status === "failed"
  const isCompleted = query.status === "completed"

  return (
    <Dialog open={open} onOpenChange={(v) => !v && onClose()}>
      <DialogContent className="sm:max-w-xl">
        <DialogHeader>
          <div className="flex items-center gap-3">
            {isFailed ? (
              <XCircle className="h-5 w-5 text-destructive shrink-0" />
            ) : isCompleted ? (
              <CheckCircle2 className="h-5 w-5 text-green-600 shrink-0" />
            ) : (
              <Clock className="h-5 w-5 text-muted-foreground shrink-0" />
            )}
            <DialogTitle className="font-mono">{query.query_name}</DialogTitle>
          </div>
          <DialogDescription className="flex items-center gap-2">
            <StatusBadge status={query.status} />
          </DialogDescription>
        </DialogHeader>

        <Separator />

        {isFailed && query.error && (
          <div className="space-y-2">
            <div className="flex items-center gap-2 text-sm font-medium text-destructive">
              <AlertTriangle className="h-4 w-4" />
              Error Details
            </div>
            <div className="rounded-md bg-destructive/10 p-3 text-sm text-destructive font-mono whitespace-pre-wrap break-all max-h-60 overflow-y-auto">
              {query.error}
            </div>
          </div>
        )}

        {isCompleted && (
          <div className="space-y-3">
            <dl className="grid grid-cols-[auto_1fr] gap-x-6 gap-y-2 text-sm">
              <dt className="flex items-center gap-2 text-muted-foreground">
                <Database className="h-4 w-4" /> Rows Extracted
              </dt>
              <dd className="font-mono font-medium">
                {query.rows_extracted > 0 ? query.rows_extracted.toLocaleString() : "0"}
              </dd>

              <dt className="flex items-center gap-2 text-muted-foreground">
                <Clock className="h-4 w-4" /> Duration
              </dt>
              <dd className="font-mono">
                {query.duration_seconds > 0 ? formatDuration(query.duration_seconds) : "--"}
              </dd>

              {query.rows_extracted > 0 && query.duration_seconds > 0 && (
                <>
                  <dt className="flex items-center gap-2 text-muted-foreground">
                    <FileText className="h-4 w-4" /> Throughput
                  </dt>
                  <dd className="font-mono">
                    {formatRows(Math.round(query.rows_extracted / query.duration_seconds))}{" "}
                    rows/s
                  </dd>
                </>
              )}

              {query.table_name && (
                <>
                  <dt className="flex items-center gap-2 text-muted-foreground">
                    <Database className="h-4 w-4" /> Target Table
                  </dt>
                  <dd className="font-mono text-xs break-all">{query.table_name}</dd>
                </>
              )}

              {query.started_at && (
                <>
                  <dt className="flex items-center gap-2 text-muted-foreground">
                    <Clock className="h-4 w-4" /> Started At
                  </dt>
                  <dd className="font-mono text-xs">{formatTimestamp(query.started_at)}</dd>
                </>
              )}
            </dl>
          </div>
        )}

        {!isFailed && !isCompleted && (
          <div className="text-sm text-muted-foreground">
            Query is currently {query.status}.
            {query.started_at && (
              <span className="ml-1">Running for {liveDuration(query.started_at)}.</span>
            )}
          </div>
        )}

        <DialogFooter showCloseButton />
      </DialogContent>
    </Dialog>
  )
}

export default function EventDetailPage() {
  const { jobId } = useParams<{ jobId: string }>()
  const navigate = useNavigate()
  const [selectedQuery, setSelectedQuery] = useState<QueryResult | null>(null)
  const [retrying, setRetrying] = useState(false)
  const [, setTick] = useState(0)

  const { data: event, isLoading, error } = useQuery({
    queryKey: ["event", jobId],
    queryFn: () => getEvent(jobId!),
    enabled: !!jobId,
    refetchInterval: (query) => {
      const data = query.state.data
      return data?.status === "running" || data?.status === "pending" ? 3_000 : false
    },
  })

  // Tick every second while running so durations update in real-time
  const isRunning = event?.status === "running" || event?.status === "pending"
  useEffect(() => {
    if (!isRunning) return
    const id = setInterval(() => setTick((t) => t + 1), 1_000)
    return () => clearInterval(id)
  }, [isRunning])

  // Build a combined results list: completed/failed results + running query placeholders
  const results = useMemo(() => {
    const finished = [...(event?.results ?? [])].reverse()
    // Add placeholder entries for currently running queries not yet in results
    const resultNames = new Set(finished.map((r) => r.query_name))
    const runningPlaceholders: QueryResult[] = (event?.running_queries ?? [])
      .filter((name) => !resultNames.has(name))
      .map((name) => ({
        query_name: name,
        status: "running" as JobStatus,
        rows_extracted: 0,
        table_name: null,
        duration_seconds: 0,
        error: null,
        started_at: event?.started_at ?? null,
      }))
    return [...runningPlaceholders, ...finished]
  }, [event])

  const failedQueries = useMemo(() => {
    // Prefer the persisted list from the API
    if (event?.failed_queries && event.failed_queries.length > 0) {
      return event.failed_queries
    }
    // Fall back to computing from results (available for in-flight jobs)
    return results.filter((r) => r.status === "failed").map((r) => r.query_name)
  }, [event, results])

  async function handleRetryFailed() {
    if (!event || failedQueries.length === 0) return
    setRetrying(true)
    try {
      const resp = await triggerSync({
        country: event.country,
        stage: event.stage,
        queries: failedQueries,
      })
      toast.success(`Retrying ${failedQueries.length} failed queries`)
      navigate(`/history/${resp.job_id}`)
    } catch (err: unknown) {
      const message =
        err && typeof err === "object" && "message" in err
          ? (err as { message: string }).message
          : "Failed to trigger retry"
      toast.error(message)
      setRetrying(false)
    }
  }

  if (isLoading) {
    return (
      <div className="space-y-4">
        <Skeleton className="h-6 w-32" />
        <Skeleton className="h-48 w-full" />
        <Skeleton className="h-64 w-full" />
      </div>
    )
  }

  if (error || !event) {
    return (
      <div className="space-y-4">
        <Link to="/history" className="text-sm text-muted-foreground hover:underline">
          &larr; Back to History
        </Link>
        <p className="py-8 text-center text-muted-foreground">
          Job not found
        </p>
      </div>
    )
  }

  const queriesDone = event.queries_completed + (event.queries_failed ?? 0)
  const queriesRunning = event.queries_running ?? 0
  const queriesPending = Math.max(0, event.queries_total - queriesDone - queriesRunning)
  const progressPct =
    event.queries_total > 0
      ? Math.round((queriesDone / event.queries_total) * 100)
      : 0

  return (
    <div className="space-y-6">
      <Link to="/history" className="text-sm text-muted-foreground hover:underline">
        &larr; Back to History
      </Link>

      {/* Job Summary Card */}
      <Card>
        <CardHeader className="flex flex-row items-center gap-4">
          <StatusBadge status={event.status} className="text-sm px-3 py-1" />
          <CardTitle className="text-xl">
            {event.country.charAt(0).toUpperCase() + event.country.slice(1)}
          </CardTitle>
        </CardHeader>
        <CardContent className="space-y-4">
          <dl className="grid grid-cols-2 gap-x-8 gap-y-2 text-sm">
            <dt className="text-muted-foreground">Stage</dt>
            <dd>{event.stage}</dd>
            <dt className="text-muted-foreground">Tag</dt>
            <dd className="font-mono">{event.tag}</dd>
            <dt className="text-muted-foreground">Triggered by</dt>
            <dd>{event.triggered_by}</dd>
            <dt className="text-muted-foreground">Started</dt>
            <dd>{formatTimestamp(event.started_at)}</dd>
            <dt className="text-muted-foreground">Completed</dt>
            <dd>{formatTimestamp(event.completed_at)}</dd>
            <dt className="text-muted-foreground">Duration</dt>
            <dd>{computeDuration(event.started_at, event.completed_at)}</dd>
            {(event.total_rows_extracted ?? 0) > 0 && (
              <>
                <dt className="text-muted-foreground">Total Rows</dt>
                <dd className="font-mono">{formatRows(event.total_rows_extracted ?? 0)}</dd>
              </>
            )}
          </dl>

          <Separator />

          {/* Progress section */}
          <div className="space-y-3">
            {/* Breakdown badges */}
            <div className="flex items-center justify-between">
              <div className="flex items-center gap-3 text-sm">
                <span className="flex items-center gap-1.5">
                  <Layers className="h-4 w-4 text-muted-foreground" />
                  {queriesDone}/{event.queries_total} queries
                </span>
                {event.queries_completed > 0 && (
                  <span className="inline-flex items-center gap-1 rounded-full bg-green-500/10 px-2 py-0.5 text-xs font-medium text-green-700 dark:text-green-400">
                    <CheckCircle2 className="h-3 w-3" />
                    {event.queries_completed} done
                  </span>
                )}
                {queriesRunning > 0 && (
                  <span className="inline-flex items-center gap-1 rounded-full bg-blue-500/10 px-2 py-0.5 text-xs font-medium text-blue-700 dark:text-blue-400">
                    <Loader2 className="h-3 w-3 animate-spin" />
                    {queriesRunning} running
                  </span>
                )}
                {queriesPending > 0 && isRunning && (
                  <span className="inline-flex items-center gap-1 rounded-full bg-muted px-2 py-0.5 text-xs font-medium text-muted-foreground">
                    <Clock className="h-3 w-3" />
                    {queriesPending} queued
                  </span>
                )}
                {(event.queries_failed ?? 0) > 0 && (
                  <span className="inline-flex items-center gap-1 rounded-full bg-destructive/10 px-2 py-0.5 text-xs font-medium text-destructive">
                    <XCircle className="h-3 w-3" />
                    {event.queries_failed} failed
                  </span>
                )}
              </div>
              <span className="text-sm text-muted-foreground">{progressPct}%</span>
            </div>
            <Progress value={progressPct} />

            {/* Running queries list */}
            {event.status === "running" && (event.running_queries?.length ?? 0) > 0 && (
              <div className="space-y-1.5">
                {event.running_queries?.map((qName) => (
                  <div key={qName} className="flex items-center gap-2 text-sm text-blue-600 dark:text-blue-400">
                    <Loader2 className="h-3.5 w-3.5 animate-spin shrink-0" />
                    <span className="font-mono">{qName}</span>
                  </div>
                ))}
              </div>
            )}
            {/* Fallback: single current_query if running_queries not populated */}
            {event.status === "running" && (event.running_queries?.length ?? 0) === 0 && event.current_query && (
              <div className="flex items-center gap-2 text-sm text-muted-foreground">
                <Loader2 className="h-4 w-4 animate-spin" />
                <span>
                  Syncing: <span className="font-mono">{event.current_query}</span>
                </span>
              </div>
            )}
          </div>

          {(event.queries_failed ?? 0) > 0 && event.status !== "running" && (
            <div className="flex items-center justify-between gap-3 rounded-md bg-amber-500/10 border border-amber-500/20 p-3 text-sm text-amber-700 dark:text-amber-400">
              <div className="flex items-start gap-2">
                <AlertTriangle className="h-4 w-4 mt-0.5 shrink-0" />
                <span>
                  Completed with errors: {event.queries_failed} of {event.queries_total} queries failed.
                  Click on a failed query below for details.
                </span>
              </div>
              <Button
                size="sm"
                variant="outline"
                className="shrink-0 border-amber-500/30 text-amber-700 hover:bg-amber-500/10 dark:text-amber-400"
                disabled={retrying || failedQueries.length === 0}
                onClick={handleRetryFailed}
                title={
                  failedQueries.length === 0 && (event.queries_failed ?? 0) > 0
                    ? "Loading failed query details..."
                    : undefined
                }
              >
                {retrying ? (
                  <Loader2 className="mr-2 h-3 w-3 animate-spin" />
                ) : (
                  <RotateCcw className="mr-2 h-3 w-3" />
                )}
                Retry {failedQueries.length || event.queries_failed} Failed
              </Button>
            </div>
          )}

          {event.error && (
            <div className="rounded-md bg-destructive/10 p-3 text-sm text-destructive">
              {event.error}
            </div>
          )}
        </CardContent>
      </Card>

      {/* Query Results Table */}
      {results.length > 0 && (
        <div className="space-y-3">
          <h2 className="text-lg font-semibold">Query Results</h2>
          <DataTable
            columns={queryResultColumns}
            data={results}
            onRowClick={(result) => setSelectedQuery(result)}
            rowClassName={(result) =>
              result.status === "failed"
                ? "bg-destructive/5"
                : result.status === "running"
                ? "bg-blue-500/5"
                : ""
            }
          />
        </div>
      )}

      <QueryDetailDialog
        query={selectedQuery}
        open={selectedQuery !== null}
        onClose={() => setSelectedQuery(null)}
      />
    </div>
  )
}

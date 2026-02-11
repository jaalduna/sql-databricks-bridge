import { useMemo } from "react"
import { Link, useParams } from "react-router-dom"
import { useQuery } from "@tanstack/react-query"
import type { ColumnDef } from "@tanstack/react-table"
import { getEvent } from "@/lib/api"
import type { JobStatus, QueryResult } from "@/types/api"
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
import { Progress } from "@/components/ui/progress"
import { Separator } from "@/components/ui/separator"
import { Skeleton } from "@/components/ui/skeleton"
import { StatusBadge } from "@/components/StatusBadge"
import { DataTable } from "@/components/DataTable"

function formatRows(n: number): string {
  if (n >= 1_000_000) return `${(n / 1_000_000).toFixed(1)}M`
  if (n >= 1_000) return `${(n / 1_000).toFixed(0)}K`
  return n.toLocaleString()
}

function formatDuration(seconds: number): string {
  if (seconds < 1) return `${(seconds * 1000).toFixed(0)}ms`
  if (seconds < 60) return `${seconds.toFixed(1)}s`
  const mins = Math.floor(seconds / 60)
  const secs = Math.round(seconds % 60)
  return `${mins}m ${secs}s`
}

function formatTimestamp(dateStr: string | null): string {
  if (!dateStr) return "--"
  return new Date(dateStr).toLocaleString("en-US", {
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
  const start = new Date(started).getTime()
  const end = completed ? new Date(completed).getTime() : Date.now()
  const seconds = (end - start) / 1000
  return formatDuration(seconds)
}

const queryResultColumns: ColumnDef<QueryResult, unknown>[] = [
  {
    accessorKey: "query_name",
    header: "Query Name",
    cell: ({ getValue }) => (
      <span className="font-mono text-sm">{getValue<string>()}</span>
    ),
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
    accessorKey: "duration_seconds",
    header: () => <span className="flex justify-end">Duration</span>,
    cell: ({ getValue }) => {
      const seconds = getValue<number>()
      return (
        <span className="flex justify-end text-muted-foreground">
          {seconds > 0 ? formatDuration(seconds) : "--"}
        </span>
      )
    },
  },
]

export default function EventDetailPage() {
  const { jobId } = useParams<{ jobId: string }>()

  const { data: event, isLoading, error } = useQuery({
    queryKey: ["event", jobId],
    queryFn: () => getEvent(jobId!),
    enabled: !!jobId,
    refetchInterval: (query) => {
      const data = query.state.data
      return data?.status === "running" ? 5_000 : false
    },
  })

  const results = useMemo(() => event?.results ?? [], [event])

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

  const progressPct =
    event.queries_total > 0
      ? Math.round((event.queries_completed / event.queries_total) * 100)
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
          </dl>

          <Separator />

          <div className="space-y-2">
            <div className="flex items-center justify-between text-sm">
              <span>
                Progress: {event.queries_completed}/{event.queries_total} queries
              </span>
              <span className="text-muted-foreground">{progressPct}%</span>
            </div>
            <Progress value={progressPct} />
          </div>

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
            rowClassName={(result) => (result.error ? "bg-destructive/5" : "")}
          />
        </div>
      )}
    </div>
  )
}

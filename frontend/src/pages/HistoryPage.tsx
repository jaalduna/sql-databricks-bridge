import { useMemo, useState } from "react"
import { useNavigate } from "react-router-dom"
import { useQuery } from "@tanstack/react-query"
import type { ColumnDef, SortingState } from "@tanstack/react-table"
import { getEvents } from "@/lib/api"
import type { EventSummary, JobStatus } from "@/types/api"
import { Button } from "@/components/ui/button"
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select"
import { Skeleton } from "@/components/ui/skeleton"
import { StatusBadge } from "@/components/StatusBadge"
import { DataTable } from "@/components/DataTable"

const COUNTRIES = [
  "argentina", "bolivia", "brazil", "cam", "chile", "colombia", "ecuador", "mexico", "peru",
]

const STATUSES = ["pending", "running", "completed", "failed", "cancelled"] as const

const PAGE_SIZE = 20

function formatDate(dateStr: string): string {
  return new Date(dateStr).toLocaleDateString("en-US", {
    month: "short",
    day: "numeric",
    year: "numeric",
    hour: "2-digit",
    minute: "2-digit",
  })
}

function capitalize(s: string): string {
  return s.charAt(0).toUpperCase() + s.slice(1)
}

const historyColumns: ColumnDef<EventSummary, unknown>[] = [
  {
    accessorKey: "country",
    header: "Country",
    cell: ({ getValue }) => (
      <span className="font-medium">{capitalize(getValue<string>())}</span>
    ),
  },
  {
    accessorKey: "tag",
    header: "Tag",
    cell: ({ getValue }) => (
      <span className="font-mono text-xs text-muted-foreground">
        {getValue<string>()}
      </span>
    ),
  },
  {
    accessorKey: "triggered_by",
    header: "Triggered By",
    cell: ({ getValue }) => (
      <span className="text-muted-foreground">{getValue<string>()}</span>
    ),
  },
  {
    accessorKey: "status",
    header: "Status",
    cell: ({ getValue }) => <StatusBadge status={getValue<JobStatus>()} />,
    enableSorting: false,
  },
  {
    id: "queries",
    header: () => <span className="flex justify-end">Queries</span>,
    accessorFn: (row) => row.queries_completed,
    cell: ({ row }) => (
      <span className="flex justify-end font-mono text-sm">
        {row.original.queries_completed}/{row.original.queries_total}
      </span>
    ),
    sortingFn: "basic",
  },
  {
    accessorKey: "created_at",
    header: () => <span className="flex justify-end">Date</span>,
    cell: ({ getValue }) => (
      <span className="flex justify-end text-muted-foreground">
        {formatDate(getValue<string>())}
      </span>
    ),
  },
]

export default function HistoryPage() {
  const navigate = useNavigate()

  const [offset, setOffset] = useState(0)
  const [countryFilter, setCountryFilter] = useState<string>("all")
  const [statusFilter, setStatusFilter] = useState<string>("all")
  const [sorting, setSorting] = useState<SortingState>([])

  const { data, isLoading } = useQuery({
    queryKey: ["events", "history", countryFilter, statusFilter, offset],
    queryFn: () =>
      getEvents({
        country: countryFilter !== "all" ? countryFilter : undefined,
        status: statusFilter !== "all" ? statusFilter : undefined,
        limit: PAGE_SIZE,
        offset,
      }),
  })

  const events = useMemo(() => data?.items ?? [], [data])
  const total = data?.total ?? 0
  const totalPages = Math.ceil(total / PAGE_SIZE)
  const currentPage = Math.floor(offset / PAGE_SIZE) + 1

  const handleCountryChange = (value: string) => {
    setCountryFilter(value)
    setOffset(0)
  }

  const handleStatusChange = (value: string) => {
    setStatusFilter(value)
    setOffset(0)
  }

  return (
    <div className="space-y-4">
      {/* Filters */}
      <div className="flex gap-3">
        <Select value={countryFilter} onValueChange={handleCountryChange}>
          <SelectTrigger className="w-40">
            <SelectValue placeholder="Country" />
          </SelectTrigger>
          <SelectContent>
            <SelectItem value="all">All countries</SelectItem>
            {COUNTRIES.map((c) => (
              <SelectItem key={c} value={c}>
                {capitalize(c)}
              </SelectItem>
            ))}
          </SelectContent>
        </Select>

        <Select value={statusFilter} onValueChange={handleStatusChange}>
          <SelectTrigger className="w-36">
            <SelectValue placeholder="Status" />
          </SelectTrigger>
          <SelectContent>
            <SelectItem value="all">All statuses</SelectItem>
            {STATUSES.map((s) => (
              <SelectItem key={s} value={s}>
                {capitalize(s)}
              </SelectItem>
            ))}
          </SelectContent>
        </Select>
      </div>

      {/* Table */}
      {isLoading ? (
        <div className="space-y-2">
          {Array.from({ length: 5 }).map((_, i) => (
            <Skeleton key={i} className="h-12 w-full" />
          ))}
        </div>
      ) : events.length === 0 ? (
        <p className="py-8 text-center text-sm text-muted-foreground">
          No sync jobs found.
        </p>
      ) : (
        <>
          <DataTable
            columns={historyColumns}
            data={events}
            sorting={sorting}
            onSortingChange={setSorting}
            onRowClick={(event) => navigate(`/events/${event.job_id}`)}
          />

          {/* Pagination */}
          {totalPages > 1 && (
            <div className="flex items-center justify-between pt-2">
              <p className="text-sm text-muted-foreground">
                Showing {offset + 1}-{Math.min(offset + PAGE_SIZE, total)} of {total}
              </p>
              <div className="flex gap-1">
                <Button
                  variant="outline"
                  size="sm"
                  disabled={offset === 0}
                  onClick={() => setOffset(Math.max(0, offset - PAGE_SIZE))}
                >
                  Previous
                </Button>
                <span className="flex items-center px-3 text-sm text-muted-foreground">
                  {currentPage} / {totalPages}
                </span>
                <Button
                  variant="outline"
                  size="sm"
                  disabled={offset + PAGE_SIZE >= total}
                  onClick={() => setOffset(offset + PAGE_SIZE)}
                >
                  Next
                </Button>
              </div>
            </div>
          )}
        </>
      )}
    </div>
  )
}

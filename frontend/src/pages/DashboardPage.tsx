import { useMemo, useState } from "react"
import { useNavigate } from "react-router-dom"
import { useQuery, useMutation } from "@tanstack/react-query"
import { toast } from "sonner"
import type { ColumnDef } from "@tanstack/react-table"
import { getCountries, getEvents, getStages, triggerSync } from "@/lib/api"
import type { ApiError, EventSummary, JobStatus } from "@/types/api"
import { Button } from "@/components/ui/button"
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select"
import {
  AlertDialog,
  AlertDialogAction,
  AlertDialogCancel,
  AlertDialogContent,
  AlertDialogDescription,
  AlertDialogFooter,
  AlertDialogHeader,
  AlertDialogTitle,
} from "@/components/ui/alert-dialog"
import { Skeleton } from "@/components/ui/skeleton"
import { StatusBadge } from "@/components/StatusBadge"
import { DataTable } from "@/components/DataTable"

function timeAgo(dateStr: string): string {
  const date = new Date(dateStr)
  const now = new Date()
  const seconds = Math.floor((now.getTime() - date.getTime()) / 1000)

  if (seconds < 60) return "just now"
  const minutes = Math.floor(seconds / 60)
  if (minutes < 60) return `${minutes}m ago`
  const hours = Math.floor(minutes / 60)
  if (hours < 24) return `${hours}h ago`
  const days = Math.floor(hours / 24)
  return `${days}d ago`
}

function capitalize(s: string): string {
  return s.charAt(0).toUpperCase() + s.slice(1)
}

function todayISO(): string {
  return new Date().toISOString().slice(0, 10)
}

const recentJobColumns: ColumnDef<EventSummary, unknown>[] = [
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
    accessorKey: "status",
    header: "Status",
    cell: ({ getValue }) => <StatusBadge status={getValue<JobStatus>()} />,
    enableSorting: false,
  },
  {
    accessorKey: "triggered_by",
    header: "Triggered By",
    cell: ({ getValue }) => (
      <span className="text-muted-foreground">{getValue<string>()}</span>
    ),
  },
  {
    accessorKey: "created_at",
    header: () => <span className="flex justify-end">When</span>,
    cell: ({ getValue }) => (
      <span className="flex justify-end text-muted-foreground">
        {timeAgo(getValue<string>())}
      </span>
    ),
  },
]

export default function DashboardPage() {
  const navigate = useNavigate()

  // Form state
  const [selectedCountry, setSelectedCountry] = useState("")
  const [selectedStage, setSelectedStage] = useState("")
  const [confirmOpen, setConfirmOpen] = useState(false)

  // Fetch countries
  const { data: countriesData, isLoading: loadingCountries } = useQuery({
    queryKey: ["countries"],
    queryFn: getCountries,
  })
  const countries = countriesData?.countries ?? []
  const selectedCountryInfo = countries.find((c) => c.code === selectedCountry)

  // Fetch stages
  const { data: stagesData, isLoading: loadingStages } = useQuery({
    queryKey: ["stages"],
    queryFn: getStages,
  })
  const stages = stagesData?.stages ?? []

  // Fetch recent jobs (refetch every 30s)
  const { data: recentJobsData, isLoading: loadingJobs } = useQuery({
    queryKey: ["events", "recent"],
    queryFn: () => getEvents({ limit: 5 }),
    refetchInterval: 30_000,
  })
  const recentJobs = useMemo(() => recentJobsData?.items ?? [], [recentJobsData])

  // Trigger mutation
  const trigger = useMutation({
    mutationFn: triggerSync,
    onSuccess: (response) => {
      toast.success(`Sync job started: ${response.job_id}`)
      navigate(`/events/${response.job_id}`)
    },
    onError: (err: ApiError) => {
      toast.error(err.message ?? "Failed to trigger sync")
    },
  })

  const canTrigger = selectedCountry !== "" && selectedStage !== ""

  // Auto-composed tag preview
  const tagPreview =
    selectedCountry && selectedStage
      ? `${selectedCountry}-${selectedStage}-${todayISO()}`
      : ""

  const handleConfirm = () => {
    setConfirmOpen(false)
    trigger.mutate({ country: selectedCountry, stage: selectedStage })
  }

  return (
    <div className="space-y-6">
      {/* Sync Trigger Card */}
      <Card>
        <CardHeader>
          <CardTitle>Trigger Data Sync</CardTitle>
        </CardHeader>
        <CardContent className="space-y-4">
          <div className="grid grid-cols-2 gap-4">
            <div className="space-y-2">
              <label htmlFor="country" className="text-sm font-medium">
                Country
              </label>
              {loadingCountries ? (
                <Skeleton className="h-10 w-full" />
              ) : (
                <Select value={selectedCountry} onValueChange={setSelectedCountry}>
                  <SelectTrigger id="country">
                    <SelectValue placeholder="Select country" />
                  </SelectTrigger>
                  <SelectContent>
                    {countries.map((c) => (
                      <SelectItem key={c.code} value={c.code}>
                        {capitalize(c.code)}
                      </SelectItem>
                    ))}
                  </SelectContent>
                </Select>
              )}
            </div>
            <div className="space-y-2">
              <label htmlFor="stage" className="text-sm font-medium">
                Stage
              </label>
              {loadingStages ? (
                <Skeleton className="h-10 w-full" />
              ) : (
                <Select value={selectedStage} onValueChange={setSelectedStage}>
                  <SelectTrigger id="stage">
                    <SelectValue placeholder="Select stage" />
                  </SelectTrigger>
                  <SelectContent>
                    {stages.map((s) => (
                      <SelectItem key={s.code} value={s.code}>
                        {s.name}
                      </SelectItem>
                    ))}
                  </SelectContent>
                </Select>
              )}
            </div>
          </div>

          {tagPreview && (
            <p className="text-sm text-muted-foreground">
              Tag: <span className="font-mono">{tagPreview}</span>
            </p>
          )}

          <div className="flex justify-end">
            <Button
              size="lg"
              disabled={!canTrigger || trigger.isPending}
              onClick={() => setConfirmOpen(true)}
            >
              {trigger.isPending ? "Starting..." : "Trigger Sync"}
            </Button>
          </div>
        </CardContent>
      </Card>

      {/* Confirm Dialog */}
      <AlertDialog open={confirmOpen} onOpenChange={setConfirmOpen}>
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>Confirm Sync Trigger</AlertDialogTitle>
            <AlertDialogDescription asChild>
              <div className="space-y-3">
                <p>You are about to trigger a data sync:</p>
                <dl className="grid grid-cols-[auto_1fr] gap-x-4 gap-y-1 text-sm">
                  <dt className="font-medium">Country:</dt>
                  <dd>{capitalize(selectedCountry)}</dd>
                  <dt className="font-medium">Stage:</dt>
                  <dd>{capitalize(selectedStage)}</dd>
                  <dt className="font-medium">Tag:</dt>
                  <dd className="font-mono">{tagPreview}</dd>
                  <dt className="font-medium">Queries:</dt>
                  <dd>All ({selectedCountryInfo?.queries_count ?? "?"} queries)</dd>
                </dl>
                <p className="text-muted-foreground">
                  This will extract data from SQL Server and write to Databricks Unity Catalog.
                </p>
              </div>
            </AlertDialogDescription>
          </AlertDialogHeader>
          <AlertDialogFooter>
            <AlertDialogCancel>Cancel</AlertDialogCancel>
            <AlertDialogAction onClick={handleConfirm}>
              Confirm &amp; Start
            </AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>

      {/* Recent Jobs */}
      <div className="space-y-3">
        <h2 className="text-lg font-semibold">Recent Jobs</h2>
        {loadingJobs ? (
          <div className="space-y-2">
            {Array.from({ length: 3 }).map((_, i) => (
              <Skeleton key={i} className="h-12 w-full" />
            ))}
          </div>
        ) : recentJobs.length === 0 ? (
          <p className="text-sm text-muted-foreground">No sync jobs yet.</p>
        ) : (
          <DataTable
            columns={recentJobColumns}
            data={recentJobs}
            onRowClick={(job) => navigate(`/events/${job.job_id}`)}
          />
        )}
      </div>
    </div>
  )
}

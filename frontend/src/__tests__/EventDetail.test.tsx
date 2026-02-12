import { describe, it, expect, vi, beforeEach } from "vitest"
import { render, screen, waitFor } from "@testing-library/react"
import { MemoryRouter, Route, Routes } from "react-router-dom"
import { QueryClient, QueryClientProvider } from "@tanstack/react-query"

// Mock MSAL
vi.mock("@azure/msal-react", () => ({
  useIsAuthenticated: vi.fn(() => true),
  useMsal: vi.fn(() => ({
    instance: {},
    accounts: [{ username: "test@test.com", name: "Test User" }],
    inProgress: "none",
  })),
  MsalProvider: ({ children }: { children: React.ReactNode }) => <>{children}</>,
}))

// Mock API (named exports)
const mockGetEvent = vi.fn()
vi.mock("@/lib/api", () => ({
  getEvent: (...args: unknown[]) => mockGetEvent(...args),
  setTokenProvider: vi.fn(),
  api: { interceptors: { request: { use: vi.fn() }, response: { use: vi.fn() } } },
}))

import EventDetailPage from "@/pages/EventDetailPage"

function createQueryClient() {
  return new QueryClient({
    defaultOptions: { queries: { retry: false } },
  })
}

function renderEventDetail(jobId = "job-123") {
  const qc = createQueryClient()
  return render(
    <QueryClientProvider client={qc}>
      <MemoryRouter initialEntries={[`/events/${jobId}`]}>
        <Routes>
          <Route path="/events/:jobId" element={<EventDetailPage />} />
          <Route path="/history" element={<div>History Page</div>} />
        </Routes>
      </MemoryRouter>
    </QueryClientProvider>,
  )
}

const baseEvent = {
  job_id: "job-123",
  country: "bolivia",
  stage: "calibracion",
  tag: "bolivia-calibracion-2026-02-09",
  created_at: "2026-02-09T14:30:00Z",
  started_at: "2026-02-09T14:30:01Z",
  triggered_by: "test@test.com",
  current_query: null,
}

describe("EventDetailPage", () => {
  beforeEach(() => {
    vi.clearAllMocks()
  })

  it("shows loading skeleton initially", () => {
    mockGetEvent.mockReturnValue(new Promise(() => {}))
    renderEventDetail()
    expect(screen.queryByText("Query Results")).not.toBeInTheDocument()
  })

  it("shows event metadata when loaded", async () => {
    mockGetEvent.mockResolvedValue({
      ...baseEvent,
      status: "completed",
      queries_total: 3,
      queries_completed: 3,
      queries_failed: 0,
      completed_at: "2026-02-09T14:45:23Z",
      error: null,
      results: [],
    })

    renderEventDetail()

    await waitFor(() => {
      expect(screen.getByText("Bolivia")).toBeInTheDocument()
      expect(screen.getByText("test@test.com")).toBeInTheDocument()
      expect(screen.getByText(/3\/3 queries/)).toBeInTheDocument()
    })
  })

  it("shows query results table", async () => {
    mockGetEvent.mockResolvedValue({
      ...baseEvent,
      status: "completed",
      queries_total: 2,
      queries_completed: 2,
      queries_failed: 0,
      completed_at: "2026-02-09T14:45:23Z",
      error: null,
      results: [
        {
          query_name: "j_atoscompra_new",
          status: "completed",
          rows_extracted: 1200000,
          table_name: "kpi_prd_01.bolivia.j_atoscompra_new",
          duration_seconds: 242,
          error: null,
        },
      ],
    })

    renderEventDetail()

    await waitFor(() => {
      expect(screen.getByText("Query Results")).toBeInTheDocument()
      expect(screen.getByText("j_atoscompra_new")).toBeInTheDocument()
      expect(screen.getByText("1.2M")).toBeInTheDocument()
    })
  })

  it("shows error message for not found job", async () => {
    mockGetEvent.mockRejectedValue({ error: "not_found", message: "Job not found" })

    renderEventDetail("job-nonexistent")

    await waitFor(() => {
      expect(screen.getByText("Job not found")).toBeInTheDocument()
    })
  })

  it("shows progress bar", async () => {
    mockGetEvent.mockResolvedValue({
      ...baseEvent,
      status: "running",
      country: "chile",
      queries_total: 10,
      queries_completed: 3,
      queries_failed: 0,
      completed_at: null,
      error: null,
      results: [],
    })

    renderEventDetail()

    await waitFor(() => {
      expect(screen.getByText("Progress: 3/10 queries")).toBeInTheDocument()
      expect(screen.getByText("30%")).toBeInTheDocument()
    })
  })

  it("shows error when job has error field", async () => {
    mockGetEvent.mockResolvedValue({
      ...baseEvent,
      status: "failed",
      country: "brazil",
      queries_total: 5,
      queries_completed: 2,
      queries_failed: 1,
      completed_at: "2026-02-09T14:31:00Z",
      error: "Connection timeout to SQL Server",
      results: [],
    })

    renderEventDetail()

    await waitFor(() => {
      expect(screen.getByText("Connection timeout to SQL Server")).toBeInTheDocument()
    })
  })

  it("shows current query indicator when syncing", async () => {
    mockGetEvent.mockResolvedValue({
      ...baseEvent,
      status: "running",
      queries_total: 10,
      queries_completed: 3,
      queries_failed: 0,
      completed_at: null,
      error: null,
      current_query: "j_atoscompra_new",
      results: [],
    })

    renderEventDetail()

    await waitFor(() => {
      expect(screen.getByText(/Syncing:/)).toBeInTheDocument()
      expect(screen.getByText("j_atoscompra_new")).toBeInTheDocument()
    })
  })

  it("hides current query indicator when completed", async () => {
    mockGetEvent.mockResolvedValue({
      ...baseEvent,
      status: "completed",
      queries_total: 10,
      queries_completed: 10,
      queries_failed: 0,
      completed_at: "2026-02-09T14:45:23Z",
      error: null,
      current_query: null,
      results: [],
    })

    renderEventDetail()

    await waitFor(() => {
      expect(screen.getByText("100%")).toBeInTheDocument()
    })
    expect(screen.queryByText(/Syncing:/)).not.toBeInTheDocument()
  })
})

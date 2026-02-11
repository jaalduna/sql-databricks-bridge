import { describe, it, expect, vi, beforeEach } from "vitest"
import { render, screen, waitFor, fireEvent } from "@testing-library/react"
import userEvent from "@testing-library/user-event"
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
const mockGetEvents = vi.fn()
vi.mock("@/lib/api", () => ({
  getEvents: (...args: unknown[]) => mockGetEvents(...args),
  setTokenProvider: vi.fn(),
  api: { interceptors: { request: { use: vi.fn() }, response: { use: vi.fn() } } },
}))

import HistoryPage from "@/pages/HistoryPage"

const makeEvent = (overrides: Record<string, unknown> = {}) => ({
  job_id: "job-1",
  status: "completed",
  country: "bolivia",
  queries_total: 2,
  queries_completed: 2,
  queries_failed: 0,
  created_at: "2026-02-09T14:30:00Z",
  started_at: null,
  completed_at: null,
  triggered_by: "test@test.com",
  error: null,
  ...overrides,
})

function createQueryClient() {
  return new QueryClient({
    defaultOptions: { queries: { retry: false } },
  })
}

function renderHistory() {
  const qc = createQueryClient()
  return render(
    <QueryClientProvider client={qc}>
      <MemoryRouter>
        <HistoryPage />
      </MemoryRouter>
    </QueryClientProvider>,
  )
}

function renderHistoryWithRoutes() {
  const qc = createQueryClient()
  return render(
    <QueryClientProvider client={qc}>
      <MemoryRouter initialEntries={["/history"]}>
        <Routes>
          <Route path="/history" element={<HistoryPage />} />
          <Route path="/events/:jobId" element={<div>Event Detail Page</div>} />
        </Routes>
      </MemoryRouter>
    </QueryClientProvider>,
  )
}

/** Open a Radix Select and pick an option by visible text. */
async function selectOption(triggerText: string, optionText: string) {
  const trigger = screen.getByText(triggerText)
  fireEvent.pointerDown(trigger, { button: 0, ctrlKey: false, pointerType: "mouse" })
  await waitFor(() => {
    expect(screen.getByRole("option", { name: optionText })).toBeInTheDocument()
  })
  fireEvent.click(screen.getByRole("option", { name: optionText }))
}

describe("HistoryPage", () => {
  beforeEach(() => {
    vi.clearAllMocks()
  })

  it("shows loading state initially", () => {
    mockGetEvents.mockReturnValue(new Promise(() => {}))
    renderHistory()
    expect(screen.getByText("All countries")).toBeInTheDocument()
    expect(screen.getByText("All statuses")).toBeInTheDocument()
  })

  it("renders events table with data", async () => {
    mockGetEvents.mockResolvedValue({
      items: [
        makeEvent({
          job_id: "job-1",
          country: "bolivia",
          queries_total: 39,
          queries_completed: 39,
          triggered_by: "joaquin@test.com",
        }),
        makeEvent({
          job_id: "job-2",
          status: "failed",
          country: "chile",
          queries_total: 10,
          queries_completed: 5,
          queries_failed: 1,
          triggered_by: "francisco@test.com",
          error: "timeout",
        }),
      ],
      total: 2,
      limit: 20,
      offset: 0,
    })

    renderHistory()

    await waitFor(() => {
      expect(screen.getByText("Bolivia")).toBeInTheDocument()
      expect(screen.getByText("Chile")).toBeInTheDocument()
      expect(screen.getByText("joaquin@test.com")).toBeInTheDocument()
      expect(screen.getByText("francisco@test.com")).toBeInTheDocument()
    })
  })

  it("shows empty state when no events", async () => {
    mockGetEvents.mockResolvedValue({
      items: [],
      total: 0,
      limit: 20,
      offset: 0,
    })

    renderHistory()

    await waitFor(() => {
      expect(screen.getByText("No sync jobs found.")).toBeInTheDocument()
    })
  })

  it("renders status badges correctly", async () => {
    mockGetEvents.mockResolvedValue({
      items: [makeEvent()],
      total: 1,
      limit: 20,
      offset: 0,
    })

    renderHistory()

    await waitFor(() => {
      expect(screen.getByText("Completed")).toBeInTheDocument()
    })
  })

  it("renders pagination when total exceeds page size", async () => {
    mockGetEvents.mockResolvedValue({
      items: Array.from({ length: 20 }, (_, i) => makeEvent({ job_id: `job-${i}` })),
      total: 50,
      limit: 20,
      offset: 0,
    })

    renderHistory()

    await waitFor(() => {
      expect(screen.getByText(/Showing 1-20 of 50/)).toBeInTheDocument()
      expect(screen.getByRole("button", { name: /next/i })).toBeEnabled()
      expect(screen.getByRole("button", { name: /previous/i })).toBeDisabled()
    })
  })

  it("calls API with country filter when country is selected", async () => {
    mockGetEvents.mockResolvedValue({
      items: [makeEvent()],
      total: 1,
      limit: 20,
      offset: 0,
    })

    renderHistory()

    await waitFor(() => {
      expect(screen.getByText("Bolivia")).toBeInTheDocument()
    })

    expect(mockGetEvents).toHaveBeenCalledWith(
      expect.objectContaining({ country: undefined, status: undefined }),
    )

    await selectOption("All countries", "Chile")

    await waitFor(() => {
      expect(mockGetEvents).toHaveBeenCalledWith(
        expect.objectContaining({ country: "chile" }),
      )
    })
  })

  it("calls API with status filter when status is selected", async () => {
    mockGetEvents.mockResolvedValue({
      items: [makeEvent()],
      total: 1,
      limit: 20,
      offset: 0,
    })

    renderHistory()

    await waitFor(() => {
      expect(screen.getByText("Bolivia")).toBeInTheDocument()
    })

    await selectOption("All statuses", "Failed")

    await waitFor(() => {
      expect(mockGetEvents).toHaveBeenCalledWith(
        expect.objectContaining({ status: "failed" }),
      )
    })
  })

  it("resets to page 1 when filter changes", async () => {
    mockGetEvents.mockResolvedValue({
      items: Array.from({ length: 20 }, (_, i) => makeEvent({ job_id: `job-${i}` })),
      total: 50,
      limit: 20,
      offset: 0,
    })

    renderHistory()
    const user = userEvent.setup()

    await waitFor(() => {
      expect(screen.getByText(/Showing 1-20 of 50/)).toBeInTheDocument()
    })

    await user.click(screen.getByRole("button", { name: /next/i }))

    await waitFor(() => {
      expect(mockGetEvents).toHaveBeenCalledWith(
        expect.objectContaining({ offset: 20 }),
      )
    })

    await selectOption("All countries", "Bolivia")

    await waitFor(() => {
      expect(mockGetEvents).toHaveBeenCalledWith(
        expect.objectContaining({ country: "bolivia", offset: 0 }),
      )
    })
  })

  it("navigates to event detail when clicking a row", async () => {
    mockGetEvents.mockResolvedValue({
      items: [makeEvent({ job_id: "job-42" })],
      total: 1,
      limit: 20,
      offset: 0,
    })

    renderHistoryWithRoutes()
    const user = userEvent.setup()

    await waitFor(() => {
      expect(screen.getByText("Bolivia")).toBeInTheDocument()
    })

    await user.click(screen.getByText("Bolivia"))

    await waitFor(() => {
      expect(screen.getByText("Event Detail Page")).toBeInTheDocument()
    })
  })

  it("advances pages with Next button", async () => {
    mockGetEvents.mockResolvedValue({
      items: Array.from({ length: 20 }, (_, i) => makeEvent({ job_id: `job-${i}` })),
      total: 50,
      limit: 20,
      offset: 0,
    })

    renderHistory()
    const user = userEvent.setup()

    await waitFor(() => {
      expect(screen.getByText(/Showing 1-20 of 50/)).toBeInTheDocument()
    })

    await user.click(screen.getByRole("button", { name: /next/i }))

    await waitFor(() => {
      expect(mockGetEvents).toHaveBeenCalledWith(
        expect.objectContaining({ offset: 20 }),
      )
    })
  })

  it("handles API error gracefully", async () => {
    mockGetEvents.mockRejectedValue(new Error("Network error"))

    renderHistory()

    await waitFor(() => {
      expect(screen.getByText("No sync jobs found.")).toBeInTheDocument()
    })
  })
})

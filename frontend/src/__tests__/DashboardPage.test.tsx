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

// Mock sonner toast
const mockToastSuccess = vi.fn()
const mockToastError = vi.fn()
vi.mock("sonner", () => ({
  toast: Object.assign(vi.fn(), {
    success: (...args: unknown[]) => mockToastSuccess(...args),
    error: (...args: unknown[]) => mockToastError(...args),
  }),
}))

// Mock API (named exports used by TanStack Query)
const mockGetCountries = vi.fn()
const mockGetStages = vi.fn()
const mockGetEvents = vi.fn()
const mockTriggerSync = vi.fn()

vi.mock("@/lib/api", () => ({
  getCountries: (...args: unknown[]) => mockGetCountries(...args),
  getStages: (...args: unknown[]) => mockGetStages(...args),
  getEvents: (...args: unknown[]) => mockGetEvents(...args),
  triggerSync: (...args: unknown[]) => mockTriggerSync(...args),
  setTokenProvider: vi.fn(),
  api: { interceptors: { request: { use: vi.fn() }, response: { use: vi.fn() } } },
}))

import DashboardPage from "@/pages/DashboardPage"

const DEFAULT_COUNTRIES = {
  countries: [
    { code: "bolivia", queries: ["q1", "q2"], queries_count: 2 },
    { code: "brazil", queries: ["q1"], queries_count: 1 },
    { code: "chile", queries: ["q1", "q2", "q3"], queries_count: 3 },
  ],
}

const DEFAULT_STAGES = {
  stages: [
    { code: "calibracion", name: "Calibracion" },
    { code: "mtr", name: "MTR" },
    { code: "inicio", name: "Inicio" },
  ],
}

const DEFAULT_EVENTS = {
  items: [
    {
      job_id: "job-1",
      status: "completed",
      country: "bolivia",
      stage: "calibracion",
      tag: "bolivia-calibracion-2026-02-12",
      queries_total: 2,
      queries_completed: 2,
      queries_failed: 0,
      created_at: new Date().toISOString(),
      started_at: new Date().toISOString(),
      completed_at: new Date().toISOString(),
      triggered_by: "test@test.com",
      error: null,
      current_query: null,
    },
  ],
  total: 1,
  limit: 5,
  offset: 0,
}

function createQueryClient() {
  return new QueryClient({
    defaultOptions: {
      queries: { retry: false },
      mutations: { retry: false },
    },
  })
}

function renderDashboard() {
  const queryClient = createQueryClient()
  return render(
    <QueryClientProvider client={queryClient}>
      <MemoryRouter>
        <DashboardPage />
      </MemoryRouter>
    </QueryClientProvider>,
  )
}

function renderDashboardWithRoutes() {
  const queryClient = createQueryClient()
  return render(
    <QueryClientProvider client={queryClient}>
      <MemoryRouter initialEntries={["/dashboard"]}>
        <Routes>
          <Route path="/dashboard" element={<DashboardPage />} />
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

describe("DashboardPage", () => {
  beforeEach(() => {
    vi.clearAllMocks()
    mockGetCountries.mockResolvedValue(DEFAULT_COUNTRIES)
    mockGetStages.mockResolvedValue(DEFAULT_STAGES)
    mockGetEvents.mockResolvedValue(DEFAULT_EVENTS)
    mockTriggerSync.mockResolvedValue({
      job_id: "job-new",
      status: "pending",
      country: "bolivia",
      stage: "calibracion",
      tag: "bolivia-calibracion-2026-02-12",
      queries: ["q1", "q2"],
      queries_count: 2,
      created_at: new Date().toISOString(),
      triggered_by: "test@test.com",
    })
  })

  it("renders the trigger card title", async () => {
    renderDashboard()
    expect(screen.getByText("Trigger Data Sync")).toBeInTheDocument()
  })

  it("renders country selector after loading", async () => {
    renderDashboard()
    await waitFor(() => {
      expect(screen.getByText("Select country")).toBeInTheDocument()
    })
  })

  it("renders the stage selector after loading", async () => {
    renderDashboard()
    await waitFor(() => {
      expect(screen.getByText("Select stage")).toBeInTheDocument()
    })
  })

  it("trigger button is disabled when form is incomplete", async () => {
    renderDashboard()
    await waitFor(() => {
      const triggerBtn = screen.getByRole("button", { name: /trigger sync/i })
      expect(triggerBtn).toBeDisabled()
    })
  })

  it("renders recent jobs section", async () => {
    renderDashboard()
    await waitFor(() => {
      expect(screen.getByText("Recent Jobs")).toBeInTheDocument()
    })
  })

  it("loads and displays recent jobs from API", async () => {
    renderDashboard()
    await waitFor(() => {
      expect(screen.getByText("Bolivia")).toBeInTheDocument()
      expect(screen.getByText("Completed")).toBeInTheDocument()
    })
  })

  it("has a disabled trigger button that requires country and stage", async () => {
    renderDashboard()

    await waitFor(() => {
      expect(screen.getByText("Select country")).toBeInTheDocument()
    })

    const triggerBtn = screen.getByRole("button", { name: /trigger sync/i })
    expect(triggerBtn).toBeDisabled()

    // Select only country — button should still be disabled
    await selectOption("Select country", "Bolivia")
    expect(triggerBtn).toBeDisabled()
  })

  it("opens confirmation dialog when trigger button is clicked", async () => {
    renderDashboard()
    const user = userEvent.setup()

    await waitFor(() => {
      expect(screen.getByText("Select country")).toBeInTheDocument()
    })

    await selectOption("Select country", "Bolivia")
    await selectOption("Select stage", "Calibracion")

    const triggerBtn = screen.getByRole("button", { name: /trigger sync/i })
    expect(triggerBtn).toBeEnabled()
    await user.click(triggerBtn)

    await waitFor(() => {
      expect(screen.getByText("Confirm Sync Trigger")).toBeInTheDocument()
      expect(screen.getByText(/You are about to trigger a data sync/)).toBeInTheDocument()
    })
  })

  it("cancels confirmation dialog without triggering", async () => {
    renderDashboard()
    const user = userEvent.setup()

    await waitFor(() => {
      expect(screen.getByText("Select country")).toBeInTheDocument()
    })

    await selectOption("Select country", "Bolivia")
    await selectOption("Select stage", "Calibracion")

    await user.click(screen.getByRole("button", { name: /trigger sync/i }))

    await waitFor(() => {
      expect(screen.getByText("Confirm Sync Trigger")).toBeInTheDocument()
    })

    await user.click(screen.getByRole("button", { name: /cancel/i }))

    await waitFor(() => {
      expect(screen.queryByText("Confirm Sync Trigger")).not.toBeInTheDocument()
    })
    expect(mockTriggerSync).not.toHaveBeenCalled()
  })

  it("calls triggerSync and shows success toast on confirm", async () => {
    renderDashboardWithRoutes()
    const user = userEvent.setup()

    await waitFor(() => {
      expect(screen.getByText("Select country")).toBeInTheDocument()
    })

    await selectOption("Select country", "Bolivia")
    await selectOption("Select stage", "Calibracion")

    await user.click(screen.getByRole("button", { name: /trigger sync/i }))

    await waitFor(() => {
      expect(screen.getByText("Confirm Sync Trigger")).toBeInTheDocument()
    })

    await user.click(screen.getByRole("button", { name: /confirm/i }))

    await waitFor(() => {
      expect(mockTriggerSync).toHaveBeenCalled()
      expect(mockTriggerSync.mock.calls[0][0]).toEqual({
        country: "bolivia",
        stage: "calibracion",
      })
    })

    await waitFor(() => {
      expect(mockToastSuccess).toHaveBeenCalledWith("Sync job started: job-new")
    })

    await waitFor(() => {
      expect(screen.getByText("Event Detail Page")).toBeInTheDocument()
    })
  })

  it("shows error toast when trigger fails", async () => {
    mockTriggerSync.mockRejectedValue({ message: "Insufficient permissions" })

    renderDashboard()
    const user = userEvent.setup()

    await waitFor(() => {
      expect(screen.getByText("Select country")).toBeInTheDocument()
    })

    await selectOption("Select country", "Bolivia")
    await selectOption("Select stage", "Calibracion")

    await user.click(screen.getByRole("button", { name: /trigger sync/i }))

    await waitFor(() => {
      expect(screen.getByText("Confirm Sync Trigger")).toBeInTheDocument()
    })

    await user.click(screen.getByRole("button", { name: /confirm/i }))

    await waitFor(() => {
      expect(mockTriggerSync).toHaveBeenCalled()
      expect(mockToastError).toHaveBeenCalledWith("Insufficient permissions")
    })
  })

  it("shows generic error toast when trigger fails without message", async () => {
    mockTriggerSync.mockRejectedValue({})

    renderDashboard()
    const user = userEvent.setup()

    await waitFor(() => {
      expect(screen.getByText("Select country")).toBeInTheDocument()
    })

    await selectOption("Select country", "Bolivia")
    await selectOption("Select stage", "Calibracion")

    await user.click(screen.getByRole("button", { name: /trigger sync/i }))

    await waitFor(() => {
      expect(screen.getByText("Confirm Sync Trigger")).toBeInTheDocument()
    })

    await user.click(screen.getByRole("button", { name: /confirm/i }))

    await waitFor(() => {
      expect(mockToastError).toHaveBeenCalledWith("Failed to trigger sync")
    })
  })

  it("shows empty state when no recent jobs", async () => {
    mockGetEvents.mockResolvedValue({
      items: [],
      total: 0,
      limit: 5,
      offset: 0,
    })

    renderDashboard()

    await waitFor(() => {
      expect(screen.getByText("No sync jobs yet.")).toBeInTheDocument()
    })
  })

  it("navigates to event detail when clicking a recent job row", async () => {
    renderDashboardWithRoutes()

    await waitFor(() => {
      expect(screen.getByText("Bolivia")).toBeInTheDocument()
    })

    const user = userEvent.setup()
    await user.click(screen.getByText("Bolivia"))

    await waitFor(() => {
      expect(screen.getByText("Event Detail Page")).toBeInTheDocument()
    })
  })
})

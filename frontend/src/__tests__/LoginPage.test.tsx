import { describe, it, expect, vi, beforeEach } from "vitest"
import { render, screen } from "@testing-library/react"
import userEvent from "@testing-library/user-event"
import { MemoryRouter, Route, Routes } from "react-router-dom"

// Mock MSAL - use dynamic imports so we can change return values per test
const mockLogin = vi.fn()
const mockUseIsAuthenticated = vi.fn(() => false)
vi.mock("@azure/msal-react", () => ({
  useIsAuthenticated: () => mockUseIsAuthenticated(),
  useMsal: vi.fn(() => ({
    instance: { loginRedirect: mockLogin },
    accounts: [],
    inProgress: "none",
  })),
  MsalProvider: ({ children }: { children: React.ReactNode }) => <>{children}</>,
}))

import LoginPage from "@/pages/LoginPage"

function renderLogin() {
  return render(
    <MemoryRouter>
      <LoginPage />
    </MemoryRouter>,
  )
}

function renderLoginWithRoutes() {
  return render(
    <MemoryRouter initialEntries={["/"]}>
      <Routes>
        <Route path="/" element={<LoginPage />} />
        <Route path="/dashboard" element={<div>Dashboard Page</div>} />
      </Routes>
    </MemoryRouter>,
  )
}

describe("LoginPage", () => {
  beforeEach(() => {
    vi.clearAllMocks()
    mockUseIsAuthenticated.mockReturnValue(false)
  })

  it("renders the app title", () => {
    renderLogin()
    expect(screen.getByText("SQL-Databricks Bridge")).toBeInTheDocument()
  })

  it("renders the description text", () => {
    renderLogin()
    expect(screen.getByText("Data sync operations for LATAM countries")).toBeInTheDocument()
  })

  it("renders the sign-in button", () => {
    renderLogin()
    expect(screen.getByRole("button", { name: /sign in with microsoft/i })).toBeInTheDocument()
  })

  it("calls MSAL login on button click", async () => {
    renderLogin()
    const user = userEvent.setup()
    await user.click(screen.getByRole("button", { name: /sign in with microsoft/i }))
    expect(mockLogin).toHaveBeenCalled()
  })

  it("redirects to dashboard when already authenticated", () => {
    mockUseIsAuthenticated.mockReturnValue(true)
    renderLoginWithRoutes()
    expect(screen.getByText("Dashboard Page")).toBeInTheDocument()
    expect(screen.queryByText("SQL-Databricks Bridge")).not.toBeInTheDocument()
  })

  it("does not redirect when not authenticated", () => {
    mockUseIsAuthenticated.mockReturnValue(false)
    renderLoginWithRoutes()
    expect(screen.getByText("SQL-Databricks Bridge")).toBeInTheDocument()
    expect(screen.queryByText("Dashboard Page")).not.toBeInTheDocument()
  })
})

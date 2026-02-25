import { describe, it, expect, vi, beforeEach, beforeAll } from "vitest"
import { render, act, waitFor } from "@testing-library/react"
import { CalibrationProvider, useCalibrationContext } from "@/contexts/CalibrationContext"

const STORAGE_KEY = "bridge:calibration-job-ids"

// ---------------------------------------------------------------------------
// localStorage mock
// ---------------------------------------------------------------------------
// vitest 4 + jsdom: the global `localStorage` is a plain stub object without
// Storage methods when `--localstorage-file` has no valid path. We install a
// proper Map-backed mock so the context can call setItem / getItem / clear.
// ---------------------------------------------------------------------------

function makeLocalStorageMock() {
  let store: Record<string, string> = {}
  return {
    getItem: (key: string): string | null => store[key] ?? null,
    setItem: (key: string, value: string): void => {
      store[key] = String(value)
    },
    removeItem: (key: string): void => {
      delete store[key]
    },
    clear: (): void => {
      store = {}
    },
    get length() {
      return Object.keys(store).length
    },
    key: (index: number): string | null => Object.keys(store)[index] ?? null,
  }
}

const localStorageMock = makeLocalStorageMock()

beforeAll(() => {
  vi.stubGlobal("localStorage", localStorageMock)
})

// ---------------------------------------------------------------------------
// API mock
// ---------------------------------------------------------------------------

vi.mock("@/lib/api", () => ({
  getEvent: vi.fn(),
  setTokenProvider: vi.fn(),
  api: { interceptors: { request: { use: vi.fn() }, response: { use: vi.fn() } } },
}))

import { getEvent } from "@/lib/api"
const mockGetEvent = vi.mocked(getEvent)

// ---------------------------------------------------------------------------
// Helper component that exposes context values for testing
// ---------------------------------------------------------------------------

function TestConsumer({
  onRender,
}: {
  onRender: (ctx: ReturnType<typeof useCalibrationContext>) => void
}) {
  const ctx = useCalibrationContext()
  onRender(ctx)
  return null
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

describe("CalibrationContext", () => {
  beforeEach(() => {
    localStorage.clear()
    mockGetEvent.mockReset()
    // Default: return a running job so rehydration does not clear it unless overridden per test
    mockGetEvent.mockResolvedValue({ status: "running" })
  })

  // -------------------------------------------------------------------------
  // S10-001: localStorage persistence
  // -------------------------------------------------------------------------

  describe("S10-001: localStorage persistence", () => {
    it("rehydrates from localStorage on mount", () => {
      localStorage.setItem(STORAGE_KEY, JSON.stringify({ bolivia: "job-123" }))

      let capturedCtx: ReturnType<typeof useCalibrationContext> | null = null

      render(
        <CalibrationProvider>
          <TestConsumer onRender={(ctx) => { capturedCtx = ctx }} />
        </CalibrationProvider>,
      )

      expect(capturedCtx).not.toBeNull()
      expect(capturedCtx!.getJobId("bolivia")).toBe("job-123")
    })

    it("setJobId persists to localStorage", () => {
      let capturedCtx: ReturnType<typeof useCalibrationContext> | null = null

      render(
        <CalibrationProvider>
          <TestConsumer onRender={(ctx) => { capturedCtx = ctx }} />
        </CalibrationProvider>,
      )

      act(() => {
        capturedCtx!.setJobId("argentina", "job-456")
      })

      const stored = JSON.parse(localStorage.getItem(STORAGE_KEY) ?? "{}")
      expect(stored["argentina"]).toBe("job-456")
    })

    it("clearJobId persists null to localStorage", () => {
      localStorage.setItem(STORAGE_KEY, JSON.stringify({ bolivia: "job-123" }))

      let capturedCtx: ReturnType<typeof useCalibrationContext> | null = null

      render(
        <CalibrationProvider>
          <TestConsumer onRender={(ctx) => { capturedCtx = ctx }} />
        </CalibrationProvider>,
      )

      act(() => {
        capturedCtx!.clearJobId("bolivia")
      })

      const stored = JSON.parse(localStorage.getItem(STORAGE_KEY) ?? "{}")
      expect(stored["bolivia"]).toBeNull()
    })

    it("handles corrupt localStorage gracefully", () => {
      localStorage.setItem(STORAGE_KEY, "not valid json")

      let capturedCtx: ReturnType<typeof useCalibrationContext> | null = null

      expect(() => {
        render(
          <CalibrationProvider>
            <TestConsumer onRender={(ctx) => { capturedCtx = ctx }} />
          </CalibrationProvider>,
        )
      }).not.toThrow()

      expect(capturedCtx).not.toBeNull()
      expect(capturedCtx!.getJobId("bolivia")).toBeNull()
    })
  })

  // -------------------------------------------------------------------------
  // S10-002: backend rehydration on mount
  // -------------------------------------------------------------------------

  describe("S10-002: backend rehydration", () => {
    it("clears completed jobs on mount", async () => {
      localStorage.setItem(STORAGE_KEY, JSON.stringify({ bolivia: "job-done" }))
      mockGetEvent.mockResolvedValue({ status: "completed" })

      let capturedCtx: ReturnType<typeof useCalibrationContext> | null = null

      render(
        <CalibrationProvider>
          <TestConsumer onRender={(ctx) => { capturedCtx = ctx }} />
        </CalibrationProvider>,
      )

      await waitFor(() => {
        expect(capturedCtx!.getJobId("bolivia")).toBeNull()
      })
    })

    it("keeps running jobs on mount", async () => {
      localStorage.setItem(STORAGE_KEY, JSON.stringify({ bolivia: "job-running" }))
      mockGetEvent.mockResolvedValue({ status: "running" })

      let capturedCtx: ReturnType<typeof useCalibrationContext> | null = null

      render(
        <CalibrationProvider>
          <TestConsumer onRender={(ctx) => { capturedCtx = ctx }} />
        </CalibrationProvider>,
      )

      // Give the effect time to settle — the job should remain intact
      await waitFor(() => {
        expect(mockGetEvent).toHaveBeenCalledWith("job-running")
      })

      expect(capturedCtx!.getJobId("bolivia")).toBe("job-running")
    })

    it("clears 404 jobs on mount", async () => {
      localStorage.setItem(STORAGE_KEY, JSON.stringify({ bolivia: "job-gone" }))
      mockGetEvent.mockRejectedValue({ error: "not_found" })

      let capturedCtx: ReturnType<typeof useCalibrationContext> | null = null

      render(
        <CalibrationProvider>
          <TestConsumer onRender={(ctx) => { capturedCtx = ctx }} />
        </CalibrationProvider>,
      )

      await waitFor(() => {
        expect(capturedCtx!.getJobId("bolivia")).toBeNull()
      })
    })
  })
})

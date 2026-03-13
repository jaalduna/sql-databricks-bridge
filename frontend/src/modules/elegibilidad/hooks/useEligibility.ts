import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query"
import {
  getEligibilityRuns,
  getEligibilityRun,
  createEligibilityRun,
  updateEligibilityRun,
  executeEligibility,
  approveEligibility,
  uploadEligibilityFiles,
  applyMordom,
  cancelEligibility,
} from "@/lib/api"
import { ACTIVE_STATUSES } from "../types"
import type { EligibilityRunCreate, EligibilityRun } from "../types"

export function useEligibilityRuns(country?: string, period?: number) {
  return useQuery({
    queryKey: ["eligibility-runs", country, period],
    queryFn: () => getEligibilityRuns({ country, period }),
    staleTime: 30_000,
    refetchInterval: (query) => {
      const data = query.state.data
      if (!data) return false
      const hasActive = data.some(
        (run) => run.status === "pending" || ACTIVE_STATUSES.includes(run.status),
      )
      return hasActive ? 5000 : false
    },
  })
}

export function useEligibilityRun(runId: string | null) {
  return useQuery({
    queryKey: ["eligibility-run", runId],
    queryFn: () => getEligibilityRun(runId!),
    enabled: !!runId,
    refetchInterval: (query) => {
      const status = query.state.data?.status
      return status && ACTIVE_STATUSES.includes(status) ? 2000 : false
    },
  })
}

export function useCreateEligibilityRun() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: (data: EligibilityRunCreate) => createEligibilityRun(data),
    onSuccess: (run) => {
      queryClient.invalidateQueries({
        queryKey: ["eligibility-runs", run.country],
      })
    },
  })
}

export function useUpdateEligibilityRun() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: ({ runId, data }: { runId: string; data: Partial<EligibilityRun> }) =>
      updateEligibilityRun(runId, data),
    onSuccess: (run) => {
      queryClient.invalidateQueries({
        queryKey: ["eligibility-runs", run.country],
      })
    },
  })
}

export function useExecuteEligibility() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: (runId: string) => executeEligibility(runId),
    onSuccess: (run) => {
      queryClient.invalidateQueries({ queryKey: ["eligibility-runs", run.country] })
      queryClient.invalidateQueries({ queryKey: ["eligibility-run", run.run_id] })
    },
  })
}

export function useApproveEligibility() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: (runId: string) => approveEligibility(runId),
    onSuccess: (run) => {
      queryClient.invalidateQueries({ queryKey: ["eligibility-runs", run.country] })
      queryClient.invalidateQueries({ queryKey: ["eligibility-run", run.run_id] })
    },
  })
}

export function useUploadEligibilityFiles() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: ({ runId, file }: { runId: string; file: File }) =>
      uploadEligibilityFiles(runId, file),
    onSuccess: (_data, variables) => {
      queryClient.invalidateQueries({ queryKey: ["eligibility-run", variables.runId] })
      queryClient.invalidateQueries({ queryKey: ["eligibility-runs"] })
    },
  })
}

export function useApplyMordom() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: (runId: string) => applyMordom(runId),
    onSuccess: (run) => {
      queryClient.invalidateQueries({ queryKey: ["eligibility-runs", run.country] })
      queryClient.invalidateQueries({ queryKey: ["eligibility-run", run.run_id] })
    },
  })
}

export function useCancelEligibility() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: (runId: string) => cancelEligibility(runId),
    onSuccess: (run) => {
      queryClient.invalidateQueries({ queryKey: ["eligibility-runs", run.country] })
      queryClient.invalidateQueries({ queryKey: ["eligibility-run", run.run_id] })
    },
  })
}

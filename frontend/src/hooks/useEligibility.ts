import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query"
import {
  getEligibilityRuns,
  getEligibilityRun,
  createEligibilityRun,
  updateEligibilityRun,
  executeEligibility,
  executeEligibilityStage2,
  uploadEligibilityFiles,
  finalizeEligibility,
  cancelEligibility,
} from "@/lib/api"
import type { EligibilityRunCreate, EligibilityRun } from "@/types/eligibility"

export function useEligibilityRuns(country?: string, period?: number) {
  return useQuery({
    queryKey: ["eligibility-runs", country, period],
    queryFn: () => getEligibilityRuns({ country, period }),
    staleTime: 30_000,
    refetchInterval: (query) => {
      const data = query.state.data
      if (!data) return false
      const hasActive = data.some(
        (run) => run.status === "pending" || run.status.includes("_running"),
      )
      return hasActive ? 5000 : false
    },
  })
}

export function useEligibilityRun(runId: string | null) {
  const isRunning = (status?: string) => status?.includes("_running") ?? false
  return useQuery({
    queryKey: ["eligibility-run", runId],
    queryFn: () => getEligibilityRun(runId!),
    enabled: !!runId,
    refetchInterval: (query) => {
      return isRunning(query.state.data?.status) ? 2000 : false
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

export function useExecuteStage2() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: (runId: string) => executeEligibilityStage2(runId),
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

export function useFinalizeEligibility() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: (runId: string) => finalizeEligibility(runId),
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

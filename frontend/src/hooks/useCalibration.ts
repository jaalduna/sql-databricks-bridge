import { useCallback } from "react"
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query"
import { triggerCalibration, getEvent, cancelJob } from "@/lib/api"
import type { AggregationOptions, EventDetail, TriggerRequest } from "@/types/api"
import { useCalibrationContext } from "@/contexts/CalibrationContext"

export interface CalibrationOverrides {
  aggregations?: AggregationOptions
  row_limit?: number | null
  lookback_months?: number | null
  skip_sync?: boolean
  skip_copy?: boolean
  period?: string
}

export function useCalibration(country: string, stage: string) {
  const queryClient = useQueryClient()
  const ctx = useCalibrationContext()
  const activeJobId = ctx.getJobId(country)

  const trigger = useMutation({
    mutationFn: (body: TriggerRequest) => triggerCalibration(body),
    onSuccess: (data) => {
      ctx.setJobId(country, data.job_id)
    },
  })

  const job = useQuery({
    queryKey: ["calibration-job", activeJobId],
    queryFn: () => getEvent(activeJobId!),
    enabled: !!activeJobId,
    refetchInterval: (query) => {
      const status = query.state.data?.status
      if (status === "completed" || status === "failed" || status === "cancelled") {
        return false
      }
      return 2000
    },
  })

  const startCalibration = useCallback((overrides?: CalibrationOverrides) => {
    return trigger.mutateAsync({
      country,
      stage,
      period: overrides?.period,
      aggregations: overrides?.aggregations,
      row_limit: overrides?.row_limit,
      lookback_months: overrides?.lookback_months,
      skip_sync: overrides?.skip_sync,
      skip_copy: overrides?.skip_copy,
    })
  }, [trigger, country, stage])

  const cancel = useMutation({
    mutationFn: (jobId: string) => cancelJob(jobId),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["calibration-job", activeJobId] })
    },
  })

  const cancelCalibration = useCallback(() => {
    if (activeJobId) {
      cancel.mutate(activeJobId)
    }
  }, [cancel, activeJobId])

  const reset = useCallback(() => {
    ctx.clearJobId(country)
    queryClient.removeQueries({ queryKey: ["calibration-job", activeJobId] })
  }, [ctx, country, activeJobId, queryClient])

  return {
    activeJobId,
    job: job.data as EventDetail | undefined,
    isPolling: job.isFetching && !!activeJobId,
    isPending: trigger.isPending || (!!activeJobId && !job.data),
    isCancelling: cancel.isPending,
    triggerError: trigger.error,
    startCalibration,
    cancelCalibration,
    reset,
  }
}

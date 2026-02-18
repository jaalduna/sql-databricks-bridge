import { useState, useCallback } from "react"
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query"
import { triggerCalibration, getEvent } from "@/lib/api"
import type { AggregationOptions, EventDetail, TriggerRequest } from "@/types/api"

export interface CalibrationOverrides {
  aggregations?: AggregationOptions
  row_limit?: number | null
  lookback_months?: number | null
}

export function useCalibration(country: string, stage: string) {
  const queryClient = useQueryClient()
  const [activeJobId, setActiveJobId] = useState<string | null>(null)

  const trigger = useMutation({
    mutationFn: (body: TriggerRequest) => triggerCalibration(body),
    onSuccess: (data) => {
      setActiveJobId(data.job_id)
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
    trigger.mutate({
      country,
      stage,
      aggregations: overrides?.aggregations,
      row_limit: overrides?.row_limit,
      lookback_months: overrides?.lookback_months,
    })
  }, [trigger, country, stage])

  const reset = useCallback(() => {
    setActiveJobId(null)
    queryClient.removeQueries({ queryKey: ["calibration-job", activeJobId] })
  }, [activeJobId, queryClient])

  return {
    activeJobId,
    job: job.data as EventDetail | undefined,
    isPolling: job.isFetching && !!activeJobId,
    isPending: trigger.isPending,
    triggerError: trigger.error,
    startCalibration,
    reset,
  }
}

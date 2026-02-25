import { useQuery } from "@tanstack/react-query"
import { getEvents } from "@/lib/api"

export function useCalibrationHistory(country: string, enabled: boolean) {
  return useQuery({
    queryKey: ["calibration-history", country],
    queryFn: () => getEvents({ country, stage: "calibracion", limit: 20 }),
    enabled,
    staleTime: 30_000,
  })
}

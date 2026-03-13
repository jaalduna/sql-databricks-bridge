import { useQuery } from "@tanstack/react-query"
import { getJobTables } from "@/lib/api"

export function useJobTables(jobId: string | null) {
  return useQuery({
    queryKey: ["job-tables", jobId],
    queryFn: () => getJobTables(jobId!),
    enabled: !!jobId,
    staleTime: 60_000,
  })
}

import { useQuery } from "@tanstack/react-query"
import { getLastSync } from "@/lib/api"

export function useLastSync(stage?: string, minQueries?: number) {
  return useQuery({
    queryKey: ["last-sync", stage, minQueries],
    queryFn: async () => {
      const { countries } = await getLastSync(stage, minQueries)
      return countries
    },
    staleTime: 60 * 1000,
  })
}

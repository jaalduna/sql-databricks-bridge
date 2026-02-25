import { useQuery } from "@tanstack/react-query"
import { getLastSync } from "@/lib/api"

export function useLastSync() {
  return useQuery({
    queryKey: ["last-sync"],
    queryFn: async () => {
      const { countries } = await getLastSync()
      return countries
    },
    staleTime: 60 * 1000,
  })
}

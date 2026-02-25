import { useQuery } from "@tanstack/react-query"
import { getLastElegibilidad } from "@/lib/api"

export function useLastElegibilidad() {
  return useQuery({
    queryKey: ["last-elegibilidad"],
    queryFn: async () => {
      const { countries } = await getLastElegibilidad()
      return countries
    },
    staleTime: 60 * 1000,
  })
}

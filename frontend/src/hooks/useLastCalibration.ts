import { useQuery } from "@tanstack/react-query"
import { getLastCalibration } from "@/lib/api"

export function useLastCalibration() {
  return useQuery({
    queryKey: ["last-calibration"],
    queryFn: async () => {
      const { countries } = await getLastCalibration()
      return countries
    },
    staleTime: 60 * 1000,
  })
}

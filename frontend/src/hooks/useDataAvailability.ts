import { useQuery } from "@tanstack/react-query"
import { getDataAvailability } from "@/lib/api"

/**
 * Check data availability by querying on-premise SQL Server
 * for elegibilidad (mordom) and pesaje (rg_domicilios_pesos) tables.
 *
 * Returns a map: country code -> { elegibilidad, pesaje }.
 */
export function useDataAvailability(period: string) {
  return useQuery({
    queryKey: ["data-availability", period],
    queryFn: async () => {
      const { countries } = await getDataAvailability(period)
      return countries
    },
    staleTime: 60 * 1000,
  })
}

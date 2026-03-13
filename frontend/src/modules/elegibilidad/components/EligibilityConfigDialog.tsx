import { Settings } from "lucide-react"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogHeader,
  DialogTitle,
  DialogTrigger,
} from "@/components/ui/dialog"

export interface EligibilityParams {
  min_purchases: number
  min_periods: number
  mortality_threshold: number
  min_categories: number
  min_spend: number
  penet_threshold: number
  contribution_min: number
  max_gap_periods: number
  min_basket_size: number
  outlier_std_dev: number
}

export const DEFAULT_ELIGIBILITY_PARAMS: EligibilityParams = {
  min_purchases: 3,
  min_periods: 6,
  mortality_threshold: 3,
  min_categories: 2,
  min_spend: 15000,
  penet_threshold: 0.35,
  contribution_min: 0.8,
  max_gap_periods: 2,
  min_basket_size: 2,
  outlier_std_dev: 3.0,
}

const PARAM_META: Record<keyof EligibilityParams, { label: string; unit: string; step?: string }> = {
  min_purchases: { label: "Compras mínimas por período", unit: "compras" },
  min_periods: { label: "Períodos mínimos de actividad", unit: "meses" },
  mortality_threshold: { label: "Meses inactivos para declarar muerto", unit: "meses" },
  min_categories: { label: "Categorías distintas mínimas", unit: "cats" },
  min_spend: { label: "Gasto mínimo acumulado", unit: "moneda local" },
  penet_threshold: { label: "Penetración mínima del hogar", unit: "%", step: "0.01" },
  contribution_min: { label: "Contribución mínima requerida", unit: "ratio", step: "0.01" },
  max_gap_periods: { label: "Máx. períodos sin compra consecutivos", unit: "meses" },
  min_basket_size: { label: "Items mínimos por canasta", unit: "items" },
  outlier_std_dev: { label: "Desviaciones estándar para outlier", unit: "σ", step: "0.1" },
}

interface EligibilityConfigDialogProps {
  params: EligibilityParams
  onChange: (params: EligibilityParams) => void
}

export function EligibilityConfigDialog({ params, onChange }: EligibilityConfigDialogProps) {
  return (
    <Dialog>
      <DialogTrigger asChild>
        <Button variant="outline" size="icon" aria-label="Parámetros de elegibilidad">
          <Settings className="h-4 w-4" />
        </Button>
      </DialogTrigger>
      <DialogContent className="sm:max-w-md max-h-[80vh] overflow-y-auto">
        <DialogHeader>
          <DialogTitle>Parámetros de Elegibilidad</DialogTitle>
          <DialogDescription>
            Modifica los umbrales antes de ejecutar. Los valores se guardan con cada ejecución.
          </DialogDescription>
        </DialogHeader>

        <div className="space-y-3">
          {(Object.keys(PARAM_META) as (keyof EligibilityParams)[]).map((key) => {
            const meta = PARAM_META[key]
            return (
              <label key={key} className="block space-y-1">
                <div className="flex items-center justify-between">
                  <span className="text-xs text-muted-foreground">{meta.label}</span>
                  <span className="text-[10px] text-muted-foreground/60 font-mono">{meta.unit}</span>
                </div>
                <Input
                  type="number"
                  step={meta.step ?? "1"}
                  min={0}
                  value={params[key]}
                  onChange={(e) =>
                    onChange({
                      ...params,
                      [key]: e.target.value ? parseFloat(e.target.value) : 0,
                    })
                  }
                  className="h-8 text-sm font-mono"
                />
              </label>
            )
          })}
        </div>

        <div className="pt-2">
          <Button
            variant="ghost"
            size="sm"
            className="text-xs text-muted-foreground"
            onClick={() => onChange(DEFAULT_ELIGIBILITY_PARAMS)}
          >
            Restaurar valores por defecto
          </Button>
        </div>
      </DialogContent>
    </Dialog>
  )
}

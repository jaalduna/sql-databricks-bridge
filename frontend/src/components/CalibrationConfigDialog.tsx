import { Settings } from "lucide-react"
import { Button } from "@/components/ui/button"
import { Checkbox } from "@/components/ui/checkbox"
import { Input } from "@/components/ui/input"
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogHeader,
  DialogTitle,
  DialogTrigger,
} from "@/components/ui/dialog"
import type { CalibrationConfig } from "@/types/api"

interface CalibrationConfigDialogProps {
  config: CalibrationConfig
  onChange: (config: CalibrationConfig) => void
}

export function CalibrationConfigDialog({ config, onChange }: CalibrationConfigDialogProps) {
  return (
    <Dialog>
      <DialogTrigger asChild>
        <Button variant="outline" size="icon" aria-label="Calibration settings">
          <Settings className="h-4 w-4" />
        </Button>
      </DialogTrigger>
      <DialogContent className="sm:max-w-sm">
        <DialogHeader>
          <DialogTitle>Calibration Settings</DialogTitle>
          <DialogDescription>
            Configure options applied to all countries.
          </DialogDescription>
        </DialogHeader>

        <div className="space-y-4">
          {/* Aggregation checkboxes */}
          <fieldset className="space-y-2">
            <legend className="text-sm font-medium">Aggregations</legend>
            <label className="flex items-center gap-2 text-sm">
              <Checkbox
                checked={config.aggregations.region}
                onCheckedChange={(v) =>
                  onChange({
                    ...config,
                    aggregations: { ...config.aggregations, region: v === true },
                  })
                }
              />
              Region
            </label>
            <label className="flex items-center gap-2 text-sm">
              <Checkbox
                checked={config.aggregations.nivel_2}
                onCheckedChange={(v) =>
                  onChange({
                    ...config,
                    aggregations: { ...config.aggregations, nivel_2: v === true },
                  })
                }
              />
              Nivel 2
            </label>
          </fieldset>

          {/* Override parameters */}
          <fieldset className="space-y-2">
            <legend className="text-sm font-medium">Overrides</legend>
            <label className="space-y-1">
              <span className="text-xs text-muted-foreground">Top limit</span>
              <Input
                type="number"
                min={0}
                placeholder="All rows"
                value={config.row_limit ?? ""}
                onChange={(e) =>
                  onChange({
                    ...config,
                    row_limit: e.target.value ? parseInt(e.target.value, 10) : null,
                  })
                }
                className="h-8 text-sm"
              />
            </label>
            <label className="space-y-1">
              <span className="text-xs text-muted-foreground">Lookback months</span>
              <Input
                type="number"
                min={1}
                max={120}
                placeholder="Default (13)"
                value={config.lookback_months ?? ""}
                onChange={(e) =>
                  onChange({
                    ...config,
                    lookback_months: e.target.value ? parseInt(e.target.value, 10) : null,
                  })
                }
                className="h-8 text-sm"
              />
            </label>
          </fieldset>
        </div>
      </DialogContent>
    </Dialog>
  )
}

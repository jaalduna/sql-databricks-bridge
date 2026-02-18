import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select"

const MONTH_NAMES = [
  "Jan", "Feb", "Mar", "Apr", "May", "Jun",
  "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
]

function generatePeriods(count: number): string[] {
  const periods: string[] = []
  const now = new Date()
  for (let i = 0; i < count; i++) {
    const d = new Date(now.getFullYear(), now.getMonth() - i, 1)
    const yyyy = d.getFullYear()
    const mm = String(d.getMonth() + 1).padStart(2, "0")
    periods.push(`${yyyy}${mm}`)
  }
  return periods
}

function formatPeriod(p: string): string {
  const month = parseInt(p.slice(4), 10) - 1
  return `${MONTH_NAMES[month]} ${p.slice(0, 4)}`
}

interface PeriodSelectorProps {
  value: string
  onChange: (period: string) => void
}

export function PeriodSelector({ value, onChange }: PeriodSelectorProps) {
  const periods = generatePeriods(12)

  return (
    <Select value={value} onValueChange={onChange}>
      <SelectTrigger className="w-[180px]">
        <SelectValue placeholder="Select period" />
      </SelectTrigger>
      <SelectContent>
        {periods.map((p) => (
          <SelectItem key={p} value={p}>
            {formatPeriod(p)}
          </SelectItem>
        ))}
      </SelectContent>
    </Select>
  )
}

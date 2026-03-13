import { BrowserRouter, Routes, Route, Navigate } from "react-router-dom"
import { QueryClient, QueryClientProvider } from "@tanstack/react-query"
import { Toaster } from "@/components/ui/sonner"
import { MsalAuthProvider } from "@/components/MsalAuthProvider"
import { DevAuthProvider } from "@/components/DevAuthProvider"
import { ProtectedRoute } from "@/components/ProtectedRoute"
import { AuthenticatedLayout } from "@/components/AuthenticatedLayout"
import { useModuleConfig } from "@/hooks/useModuleConfig"
import LoginPage from "@/pages/LoginPage"
import DashboardPage from "@/modules/sync/pages/DashboardPage"
import HistoryPage from "@/modules/sync/pages/HistoryPage"
import EventDetailPage from "@/modules/sync/pages/EventDetailPage"
import { CalibrationPage } from "@/modules/calibracion/pages/CalibrationPage"
import { ElegibilidadPage } from "@/modules/elegibilidad/pages/ElegibilidadPage"

const AUTH_BYPASS = import.meta.env.VITE_AUTH_BYPASS === "true"

const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      staleTime: 30_000,
      retry: 1,
    },
  },
})

const isTauri = '__TAURI_INTERNALS__' in window
const basename = isTauri ? "/" : import.meta.env.BASE_URL

const AuthProvider = AUTH_BYPASS ? DevAuthProvider : MsalAuthProvider

function AppRoutes() {
  const modules = useModuleConfig()
  const defaultRoute = modules.sync ? "/dashboard" : modules.calibration ? "/calibration" : "/elegibilidad"

  return (
    <Routes>
      <Route path="/" element={<LoginPage />} />
      <Route
        element={
          <ProtectedRoute>
            <AuthenticatedLayout />
          </ProtectedRoute>
        }
      >
        {modules.sync && <Route path="/dashboard" element={<DashboardPage />} />}
        {modules.sync && <Route path="/history" element={<HistoryPage />} />}
        {modules.sync && <Route path="/events/:jobId" element={<EventDetailPage />} />}
        {modules.calibration && <Route path="/calibration" element={<CalibrationPage />} />}
        {modules.elegibilidad && <Route path="/elegibilidad" element={<ElegibilidadPage />} />}
        <Route path="*" element={<Navigate to={defaultRoute} replace />} />
      </Route>
    </Routes>
  )
}

export default function App() {
  return (
    <AuthProvider>
      <QueryClientProvider client={queryClient}>
        <BrowserRouter basename={basename}>
          <AppRoutes />
          <Toaster />
        </BrowserRouter>
      </QueryClientProvider>
    </AuthProvider>
  )
}

import { BrowserRouter, Routes, Route } from "react-router-dom"
import { QueryClient, QueryClientProvider } from "@tanstack/react-query"
import { Toaster } from "@/components/ui/sonner"
import { MsalAuthProvider } from "@/components/MsalAuthProvider"
import { DevAuthProvider } from "@/components/DevAuthProvider"
import { ProtectedRoute } from "@/components/ProtectedRoute"
import { AuthenticatedLayout } from "@/components/AuthenticatedLayout"
import LoginPage from "@/pages/LoginPage"
import DashboardPage from "@/pages/DashboardPage"
import HistoryPage from "@/pages/HistoryPage"
import EventDetailPage from "@/pages/EventDetailPage"

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

export default function App() {
  return (
    <AuthProvider>
      <QueryClientProvider client={queryClient}>
        <BrowserRouter basename={basename}>
          <Routes>
            <Route path="/" element={<LoginPage />} />
            <Route
              element={
                <ProtectedRoute>
                  <AuthenticatedLayout />
                </ProtectedRoute>
              }
            >
              <Route path="/dashboard" element={<DashboardPage />} />
              <Route path="/history" element={<HistoryPage />} />
              <Route path="/events/:jobId" element={<EventDetailPage />} />
            </Route>
          </Routes>
          <Toaster />
        </BrowserRouter>
      </QueryClientProvider>
    </AuthProvider>
  )
}

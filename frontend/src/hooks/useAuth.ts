import { createContext, useContext } from "react"

export interface AuthState {
  isAuthenticated: boolean
  user: { email: string; name: string } | null
  loading: boolean
  login: () => Promise<void>
  logout: () => Promise<void>
  getAccessToken: () => Promise<string>
}

export const AuthContext = createContext<AuthState | null>(null)

export function useAuth(): AuthState {
  const ctx = useContext(AuthContext)
  if (!ctx) throw new Error("useAuth must be used within an AuthProvider")
  return ctx
}

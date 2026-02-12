import { useState, useCallback } from "react"
import { AuthContext } from "@/hooks/useAuth"

export function DevAuthProvider({ children }: { children: React.ReactNode }) {
  const [isAuthenticated, setIsAuthenticated] = useState(false)

  const user = isAuthenticated
    ? { email: "dev@localhost", name: "Dev User" }
    : null

  const login = useCallback(async () => {
    setIsAuthenticated(true)
  }, [])

  const logout = useCallback(async () => {
    setIsAuthenticated(false)
  }, [])

  const getAccessToken = useCallback(async () => "dev-bypass-token", [])

  return (
    <AuthContext.Provider value={{ isAuthenticated, user, loading: false, login, logout, getAccessToken }}>
      {children}
    </AuthContext.Provider>
  )
}

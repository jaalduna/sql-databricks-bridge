import { useEffect, useRef } from "react"
import { useAuth } from "@/hooks/useAuth"
import { setTokenProvider } from "@/lib/api"

/**
 * Binds the MSAL token provider to the Axios interceptor.
 * Must be rendered inside MsalProvider. Only binds once.
 */
export function ApiTokenBinder() {
  const { getAccessToken, isAuthenticated } = useAuth()
  const bound = useRef(false)

  useEffect(() => {
    if (isAuthenticated && !bound.current) {
      setTokenProvider(getAccessToken)
      bound.current = true
    }
  }, [isAuthenticated, getAccessToken])

  return null
}

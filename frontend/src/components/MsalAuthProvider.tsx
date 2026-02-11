import { useCallback, useMemo } from "react"
import { PublicClientApplication } from "@azure/msal-browser"
import { MsalProvider, useMsal, useIsAuthenticated } from "@azure/msal-react"
import { InteractionStatus } from "@azure/msal-browser"
import { msalConfig, loginRequest } from "@/lib/auth-config"
import { AuthContext } from "@/hooks/useAuth"
import { ApiTokenBinder } from "@/components/ApiTokenBinder"

const msalInstance = new PublicClientApplication(msalConfig)
msalInstance.initialize()

function MsalAuthInner({ children }: { children: React.ReactNode }) {
  const { instance, accounts, inProgress } = useMsal()
  const isAuthenticated = useIsAuthenticated()

  const account = accounts[0] ?? null

  const user = useMemo(() => {
    if (!account) return null
    return { email: account.username, name: account.name ?? account.username }
  }, [account])

  const loading = inProgress !== InteractionStatus.None

  const login = useCallback(async () => {
    await instance.loginRedirect(loginRequest)
  }, [instance])

  const logout = useCallback(async () => {
    await instance.logoutRedirect({
      postLogoutRedirectUri: window.location.origin + import.meta.env.BASE_URL,
    })
  }, [instance])

  const getAccessToken = useCallback(async (): Promise<string> => {
    if (!account) throw new Error("No authenticated account")
    try {
      const response = await instance.acquireTokenSilent({ ...loginRequest, account })
      return response.accessToken
    } catch {
      const response = await instance.acquireTokenRedirect(loginRequest)
      return (response as unknown as { accessToken: string })?.accessToken ?? ""
    }
  }, [instance, account])

  return (
    <AuthContext.Provider value={{ isAuthenticated, user, loading, login, logout, getAccessToken }}>
      <ApiTokenBinder />
      {children}
    </AuthContext.Provider>
  )
}

export function MsalAuthProvider({ children }: { children: React.ReactNode }) {
  return (
    <MsalProvider instance={msalInstance}>
      <MsalAuthInner>{children}</MsalAuthInner>
    </MsalProvider>
  )
}

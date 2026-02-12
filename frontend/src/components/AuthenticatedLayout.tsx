import { useState, useEffect } from "react"
import { Outlet, NavLink } from "react-router-dom"
import { useAuth } from "@/hooks/useAuth"
import { useUpdater } from "@/hooks/useUpdater"
import { Button } from "@/components/ui/button"
import { cn } from "@/lib/utils"

export function AuthenticatedLayout() {
  const { user, logout } = useAuth()
  const { available, version: updateVersion, downloading, progress, downloadAndInstall } = useUpdater()
  const [appVersion, setAppVersion] = useState<string>(APP_VERSION)

  useEffect(() => {
    (async () => {
      try {
        const { getVersion } = await import("@tauri-apps/api/app")
        setAppVersion(await getVersion())
      } catch {
        // Not running in Tauri — keep package.json version
      }
    })()
  }, [])

  return (
    <div className="min-h-screen bg-background">
      {/* Update Banner */}
      {available && (
        <div className="bg-blue-600 text-white px-4 py-2 text-center text-sm">
          {downloading ? (
            <span>Downloading update... {progress}%</span>
          ) : (
            <span>
              Update v{updateVersion} available.{" "}
              <button
                onClick={downloadAndInstall}
                className="underline font-medium hover:text-blue-100"
              >
                Install and restart
              </button>
            </span>
          )}
        </div>
      )}

      {/* Top Bar */}
      <header className="border-b bg-card">
        <div className="mx-auto flex h-14 max-w-5xl items-center justify-between px-6">
          <div className="flex items-center gap-6">
            <span className="text-lg font-semibold">
              Bridge
              <span className="ml-2 text-xs font-normal text-muted-foreground">
                v{appVersion}
              </span>
            </span>
            <nav className="flex gap-1">
              <NavLink
                to="/dashboard"
                className={({ isActive }) =>
                  cn(
                    "rounded-md px-3 py-1.5 text-sm font-medium transition-colors",
                    isActive
                      ? "bg-primary text-primary-foreground"
                      : "text-muted-foreground hover:bg-accent hover:text-accent-foreground",
                  )
                }
              >
                Dashboard
              </NavLink>
              <NavLink
                to="/history"
                className={({ isActive }) =>
                  cn(
                    "rounded-md px-3 py-1.5 text-sm font-medium transition-colors",
                    isActive
                      ? "bg-primary text-primary-foreground"
                      : "text-muted-foreground hover:bg-accent hover:text-accent-foreground",
                  )
                }
              >
                History
              </NavLink>
            </nav>
          </div>
          <div className="flex items-center gap-3">
            <span className="text-sm text-muted-foreground">
              {user?.name ?? user?.email}
            </span>
            <Button variant="ghost" size="sm" onClick={logout}>
              Sign out
            </Button>
          </div>
        </div>
      </header>

      {/* Page content */}
      <main className="mx-auto max-w-5xl px-6 py-6">
        <Outlet />
      </main>
    </div>
  )
}

import { useState, useEffect, useCallback } from "react"

interface UpdateInfo {
  available: boolean
  version: string | null
  downloading: boolean
  progress: number
  error: string | null
}

export function useUpdater() {
  const [info, setInfo] = useState<UpdateInfo>({
    available: false,
    version: null,
    downloading: false,
    progress: 0,
    error: null,
  })

  const checkForUpdates = useCallback(async () => {
    try {
      const { check } = await import("@tauri-apps/plugin-updater")
      const update = await check()
      if (update) {
        setInfo((prev) => ({
          ...prev,
          available: true,
          version: update.version,
        }))
        return update
      }
    } catch {
      // Not in Tauri or no update available
    }
    return null
  }, [])

  const downloadAndInstall = useCallback(async () => {
    try {
      const { check } = await import("@tauri-apps/plugin-updater")
      const { relaunch } = await import("@tauri-apps/plugin-process")
      const update = await check()
      if (!update) return

      setInfo((prev) => ({ ...prev, downloading: true, progress: 0 }))

      let totalLength = 0
      let downloaded = 0

      await update.downloadAndInstall((event) => {
        if (event.event === "Started" && event.data.contentLength) {
          totalLength = event.data.contentLength
        } else if (event.event === "Progress") {
          downloaded += event.data.chunkLength
          if (totalLength > 0) {
            setInfo((prev) => ({
              ...prev,
              progress: Math.round((downloaded / totalLength) * 100),
            }))
          }
        } else if (event.event === "Finished") {
          setInfo((prev) => ({ ...prev, progress: 100 }))
        }
      })

      await relaunch()
    } catch (err) {
      setInfo((prev) => ({
        ...prev,
        downloading: false,
        error: err instanceof Error ? err.message : "Update failed",
      }))
    }
  }, [])

  useEffect(() => {
    checkForUpdates()
  }, [checkForUpdates])

  return {
    ...info,
    downloadAndInstall,
    checkForUpdates,
  }
}

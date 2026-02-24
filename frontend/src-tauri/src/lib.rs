/// Resolve the directory where config.json should live.
/// For AppImage on Linux, `current_exe()` points inside the mounted filesystem,
/// so we use the `$APPIMAGE` env var to find the real location on disk.
/// Otherwise, we use the directory containing the executable.
fn exe_adjacent_dir() -> Option<std::path::PathBuf> {
    #[cfg(target_os = "linux")]
    {
        if let Ok(appimage) = std::env::var("APPIMAGE") {
            return std::path::Path::new(&appimage).parent().map(|p| p.to_path_buf());
        }
    }

    std::env::current_exe()
        .ok()?
        .parent()
        .map(|p| p.to_path_buf())
}

/// Read external config.json with layered resolution:
/// 1. Per-user: ~/.config/com.kantar.sqldatabricksbridge/config.json (highest priority)
/// 2. Per-installation: config.json next to the executable/AppImage
/// 3. Fallback: empty object (built-in defaults apply in the frontend)
///
/// When both files exist, per-user values override per-installation values.
#[tauri::command]
fn read_config() -> Result<String, String> {
    let mut merged: serde_json::Value = serde_json::Value::Object(serde_json::Map::new());

    // Layer 1: per-installation config (next to exe/AppImage)
    if let Some(exe_dir) = exe_adjacent_dir() {
        let path = exe_dir.join("config.json");
        if path.exists() {
            if let Ok(contents) = std::fs::read_to_string(&path) {
                if let Ok(val) = serde_json::from_str::<serde_json::Value>(&contents) {
                    merge_json(&mut merged, &val);
                }
            }
        }
    }

    // Layer 2: per-user config (XDG / AppData)
    if let Some(config_dir) = dirs::config_dir() {
        let path = config_dir
            .join("com.kantar.sqldatabricksbridge")
            .join("config.json");
        if path.exists() {
            if let Ok(contents) = std::fs::read_to_string(&path) {
                if let Ok(val) = serde_json::from_str::<serde_json::Value>(&contents) {
                    merge_json(&mut merged, &val);
                }
            }
        }
    }

    serde_json::to_string(&merged).map_err(|e| e.to_string())
}

/// Deep-merge `source` into `target`. Source values win on conflict.
fn merge_json(target: &mut serde_json::Value, source: &serde_json::Value) {
    match (target, source) {
        (serde_json::Value::Object(t), serde_json::Value::Object(s)) => {
            for (key, val) in s {
                merge_json(t.entry(key.clone()).or_insert(serde_json::Value::Null), val);
            }
        }
        (target, source) => {
            *target = source.clone();
        }
    }
}

#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
    // Fix WebKitGTK EGL crash on Wayland compositors (Hyprland, Sway, etc.)
    #[cfg(target_os = "linux")]
    {
        std::env::set_var("WEBKIT_DISABLE_COMPOSITING_MODE", "1");
        std::env::set_var("WEBKIT_DISABLE_DMABUF_RENDERER", "1");
    }

    tauri::Builder::default()
        .plugin(tauri_plugin_opener::init())
        .plugin(tauri_plugin_process::init())
        .invoke_handler(tauri::generate_handler![read_config])
        .setup(|app| {
            #[cfg(desktop)]
            app.handle().plugin(tauri_plugin_updater::Builder::new().build())?;
            Ok(())
        })
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}

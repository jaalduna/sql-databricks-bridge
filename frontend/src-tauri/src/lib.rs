#[tauri::command]
fn read_config() -> Result<String, String> {
    let exe_dir = std::env::current_exe()
        .map_err(|e| e.to_string())?
        .parent()
        .ok_or("cannot get exe directory".to_string())?
        .to_path_buf();

    let config_path = exe_dir.join("config.json");

    if config_path.exists() {
        std::fs::read_to_string(&config_path).map_err(|e| e.to_string())
    } else {
        Ok("{}".to_string())
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

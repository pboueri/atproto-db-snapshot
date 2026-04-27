use anyhow::{anyhow, Result};
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

pub fn dir_size_bytes(path: &Path) -> u64 {
    let mut total: u64 = 0;
    if let Ok(rd) = std::fs::read_dir(path) {
        for entry in rd.flatten() {
            let p = entry.path();
            match entry.file_type() {
                Ok(ft) if ft.is_file() => {
                    if let Ok(meta) = entry.metadata() {
                        total += meta.len();
                    }
                }
                Ok(ft) if ft.is_dir() => {
                    total += dir_size_bytes(&p);
                }
                _ => {}
            }
        }
    }
    total
}

pub fn free_bytes_for(path: &Path) -> Option<u64> {
    let parent = if path.exists() { path } else { path.parent()? };
    let parent_str = parent.to_string_lossy();
    let out = std::process::Command::new("df")
        .args(["-Pk", parent_str.as_ref()])
        .output()
        .ok()?;
    if !out.status.success() {
        return None;
    }
    let stdout = String::from_utf8_lossy(&out.stdout);
    let line = stdout.lines().nth(1)?;
    let cols: Vec<&str> = line.split_whitespace().collect();
    let avail_kb: u64 = cols.get(3)?.parse().ok()?;
    Some(avail_kb * 1024)
}

pub struct DiskGuard {
    path: std::path::PathBuf,
    cap: u64,
    stop: Arc<AtomicBool>,
    handle: Option<std::thread::JoinHandle<()>>,
}

impl DiskGuard {
    pub fn start(path: std::path::PathBuf, cap_bytes: u64) -> Self {
        let stop = Arc::new(AtomicBool::new(false));
        let stop2 = stop.clone();
        let path2 = path.clone();
        let handle = std::thread::spawn(move || loop {
            if stop2.load(Ordering::Relaxed) {
                return;
            }
            let used = dir_size_bytes(&path2);
            if used >= cap_bytes {
                tracing::error!(
                    used_bytes = used,
                    cap_bytes,
                    path = %path2.display(),
                    "disk usage exceeded cap; aborting process"
                );
                std::process::exit(2);
            }
            tracing::debug!(
                used_bytes = used,
                cap_bytes,
                path = %path2.display(),
                "disk usage tick"
            );
            std::thread::sleep(Duration::from_secs(15));
        });
        DiskGuard {
            path,
            cap: cap_bytes,
            stop,
            handle: Some(handle),
        }
    }

    pub fn check_now(&self) -> Result<u64> {
        let used = dir_size_bytes(&self.path);
        if used >= self.cap {
            return Err(anyhow!(
                "disk usage {} exceeded cap {} at {}",
                used,
                self.cap,
                self.path.display()
            ));
        }
        Ok(used)
    }
}

impl Drop for DiskGuard {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(h) = self.handle.take() {
            let _ = h.join();
        }
    }
}

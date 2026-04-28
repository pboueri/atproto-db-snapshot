use std::env;
use std::fs;
use std::path::PathBuf;

fn main() {
    let manifest_dir = PathBuf::from(env::var("CARGO_MANIFEST_DIR").unwrap());
    let sql_dir = manifest_dir.join("src").join("sql");
    println!("cargo:rerun-if-changed={}", sql_dir.display());

    let mut stages: Vec<(u32, String, PathBuf)> = Vec::new();
    for entry in fs::read_dir(&sql_dir).expect("read src/sql") {
        let entry = entry.expect("dir entry");
        let path = entry.path();
        let name = match path.file_name().and_then(|s| s.to_str()) {
            Some(n) => n.to_string(),
            None => continue,
        };
        if !name.ends_with(".sql") {
            continue;
        }
        println!("cargo:rerun-if-changed={}", path.display());
        let stem = name.trim_end_matches(".sql");
        let (num_part, label) = match stem.split_once('_') {
            Some(parts) => parts,
            None => continue,
        };
        let num: u32 = match num_part.parse() {
            Ok(n) => n,
            Err(_) => continue,
        };
        stages.push((num, label.to_string(), path));
    }
    stages.sort_by_key(|(n, _, _)| *n);

    let mut prev: Option<u32> = None;
    for (n, label, _) in &stages {
        if let Some(p) = prev {
            if *n == p {
                panic!("duplicate sql stage prefix {n} (label {label})");
            }
        }
        prev = Some(*n);
    }

    let mut out = String::new();
    out.push_str("pub const SQL_STAGES: &[(&str, &str)] = &[\n");
    for (_, label, path) in &stages {
        out.push_str(&format!(
            "    ({:?}, include_str!({:?})),\n",
            label,
            path.to_string_lossy()
        ));
    }
    out.push_str("];\n");

    let out_dir = PathBuf::from(env::var("OUT_DIR").unwrap());
    fs::write(out_dir.join("sql_stages.rs"), out).expect("write sql_stages.rs");
}

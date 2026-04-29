use anyhow::{Context, Result};
use rocksdb::DB;
use std::path::Path;

use crate::rocks::open::open_readonly;
use crate::rocks::schema::{CF_DID_IDS, CF_LINK_TARGETS, CF_TARGET_IDS, CF_TARGET_LINKS};

/// Cheap read-only metadata snapshot of an existing rocks mirror. Reads
/// rocksdb's per-CF estimate properties out of the MANIFEST — O(1), no
/// scanning. Useful for sizing pass B before kicking off a full stage.
pub struct InspectOutcome {
    pub per_cf: Vec<CfStats>,
}

pub struct CfStats {
    pub name: &'static str,
    pub estimate_num_keys: u64,
    pub live_sst_files_size: u64,
    pub estimate_live_data_size: u64,
}

pub fn run(rocks_dir: &Path, block_cache_bytes: usize) -> Result<InspectOutcome> {
    tracing::info!(rocks_dir = %rocks_dir.display(), "opening rocks read-only for inspect");
    let db = open_readonly(rocks_dir, block_cache_bytes)?;

    let mut per_cf = Vec::new();
    for name in [
        CF_DID_IDS,
        CF_TARGET_IDS,
        CF_TARGET_LINKS,
        CF_LINK_TARGETS,
    ] {
        per_cf.push(stats_for(&db, name)?);
    }

    for s in &per_cf {
        tracing::info!(
            cf = s.name,
            estimate_num_keys = s.estimate_num_keys,
            live_sst_size_gb = format!("{:.2}", s.live_sst_files_size as f64 / 1e9),
            estimate_live_data_size_gb = format!("{:.2}", s.estimate_live_data_size as f64 / 1e9),
            "cf stats"
        );
    }
    Ok(InspectOutcome { per_cf })
}

fn stats_for(db: &DB, name: &'static str) -> Result<CfStats> {
    let cf = db
        .cf_handle(name)
        .with_context(|| format!("missing cf {name}"))?;
    Ok(CfStats {
        name,
        estimate_num_keys: prop(db, &cf, "rocksdb.estimate-num-keys")?,
        live_sst_files_size: prop(db, &cf, "rocksdb.live-sst-files-size")?,
        estimate_live_data_size: prop(db, &cf, "rocksdb.estimate-live-data-size")?,
    })
}

fn prop(db: &DB, cf: &impl rocksdb::AsColumnFamilyRef, name: &str) -> Result<u64> {
    Ok(db
        .property_int_value_cf(cf, name)
        .with_context(|| format!("read property {name}"))?
        .unwrap_or(0))
}

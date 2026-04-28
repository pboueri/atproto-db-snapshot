use anyhow::{Context, Result};
use rocksdb::{BlockBasedOptions, Cache, ColumnFamilyDescriptor, Options, DB};
use std::path::Path;

use super::schema::{CF_DID_IDS, CF_LINK_TARGETS, CF_TARGET_IDS, CF_TARGET_LINKS};

pub fn open_readonly(path: &Path, block_cache_bytes: usize) -> Result<DB> {
    let mut db_opts = Options::default();
    db_opts.create_if_missing(false);
    db_opts.create_missing_column_families(false);

    let cache_bytes = block_cache_bytes.max(8 * 1024 * 1024);
    let cache = Cache::new_lru_cache(cache_bytes);

    let mut bbt = BlockBasedOptions::default();
    bbt.set_block_cache(&cache);
    bbt.set_cache_index_and_filter_blocks(true);
    bbt.set_pin_l0_filter_and_index_blocks_in_cache(true);

    tracing::info!(
        cache_bytes,
        "configuring rocksdb block cache"
    );

    let cf_opts = || {
        let mut o = Options::default();
        o.set_block_based_table_factory(&bbt);
        o
    };
    let descriptors = vec![
        ColumnFamilyDescriptor::new(CF_DID_IDS, cf_opts()),
        ColumnFamilyDescriptor::new(CF_TARGET_IDS, cf_opts()),
        ColumnFamilyDescriptor::new(CF_TARGET_LINKS, cf_opts()),
        ColumnFamilyDescriptor::new(CF_LINK_TARGETS, cf_opts()),
    ];

    let error_if_log_file_exist = false;
    DB::open_cf_descriptors_read_only(&db_opts, path, descriptors, error_if_log_file_exist)
        .with_context(|| format!("open rocksdb read-only at {}", path.display()))
}

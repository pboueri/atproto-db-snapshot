use anyhow::{Context, Result};
use rocksdb::{ColumnFamilyDescriptor, Options, DB};
use std::path::Path;

use super::schema::{CF_DID_IDS, CF_LINK_TARGETS, CF_TARGET_IDS, CF_TARGET_LINKS};

pub fn open_readonly(path: &Path) -> Result<DB> {
    let mut db_opts = Options::default();
    db_opts.create_if_missing(false);
    db_opts.create_missing_column_families(false);

    let cf_opts = || Options::default();
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

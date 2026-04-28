//! Test helpers: synthetic constellation rocksdb fixture + TID synthesis.

use anyhow::Result;
use bincode::Options;
use chrono::{DateTime, Duration, Utc};
use rocksdb::{ColumnFamilyDescriptor, Options as RocksOptions, DB};
use std::path::Path;

use at_snapshot::rocks::schema::{
    bincode_opts, Collection, Did, DidId, DidIdValue, RKey, RPath, RecordLinkKey,
    RecordLinkTarget, RecordLinkTargets, Target, TargetId, TargetKey, TargetLinkers, CF_DID_IDS,
    CF_LINK_TARGETS, CF_TARGET_IDS, CF_TARGET_LINKS,
};

const TID_ALPHABET: &[u8; 32] = b"234567abcdefghijklmnopqrstuvwxyz";

/// Encode a microsecond timestamp into a 13-char TID rkey.
pub fn tid_for(ts: DateTime<Utc>) -> String {
    tid_with_jitter(ts, 0)
}

/// Same as `tid_for` but bumps the microsecond by `jitter`. Useful to keep
/// fixture rows distinct when many synthetic records share the same wall
/// time.
pub fn tid_with_jitter(ts: DateTime<Utc>, jitter: u64) -> String {
    let micros = ts.timestamp_micros() as u64 + jitter;
    let value = (micros << 10) | 7;
    let mut out = [0u8; 13];
    for i in (0..13).rev() {
        out[i] = TID_ALPHABET[(value >> (5 * (12 - i)) & 0x1f) as usize];
    }
    String::from_utf8(out.to_vec()).unwrap()
}

pub struct Mocker {
    db: DB,
}

impl Mocker {
    pub fn open(path: &Path) -> Result<Self> {
        let mut opts = RocksOptions::default();
        opts.create_if_missing(true);
        opts.create_missing_column_families(true);
        let cf_opts = || RocksOptions::default();
        let cfs = vec![
            ColumnFamilyDescriptor::new(CF_DID_IDS, cf_opts()),
            ColumnFamilyDescriptor::new(CF_TARGET_IDS, cf_opts()),
            ColumnFamilyDescriptor::new(CF_TARGET_LINKS, cf_opts()),
            ColumnFamilyDescriptor::new(CF_LINK_TARGETS, cf_opts()),
        ];
        let db = DB::open_cf_descriptors(&opts, path, cfs)?;
        Ok(Mocker { db })
    }

    pub fn put_actor(&self, did: &str, did_id: u64, active: bool) -> Result<()> {
        let cf = self.db.cf_handle(CF_DID_IDS).unwrap();
        let opts = bincode_opts();
        let did_obj = Did(did.to_string());
        let val = DidIdValue(DidId(did_id), active);
        // Forward: bincode(Did) -> bincode(DidIdValue)
        let k = opts.serialize(&did_obj)?;
        let v = opts.serialize(&val)?;
        self.db.put_cf(cf, k, v)?;
        // Reverse: 8-byte BE DidId -> bincode(Did)
        let kr = did_id.to_be_bytes();
        let vr = opts.serialize(&did_obj)?;
        self.db.put_cf(cf, kr, vr)?;
        Ok(())
    }

    pub fn put_target(
        &self,
        target: &str,
        collection: &str,
        rpath: &str,
        target_id: u64,
    ) -> Result<()> {
        let cf = self.db.cf_handle(CF_TARGET_IDS).unwrap();
        let opts = bincode_opts();
        let tk = TargetKey(
            Target(target.to_string()),
            Collection(collection.to_string()),
            RPath(rpath.to_string()),
        );
        let k = opts.serialize(&tk)?;
        let v = opts.serialize(&TargetId(target_id))?;
        self.db.put_cf(cf, k, v)?;
        // Reverse 8-byte BE.
        let kr = target_id.to_be_bytes();
        let vr = opts.serialize(&tk)?;
        self.db.put_cf(cf, kr, vr)?;
        Ok(())
    }

    pub fn put_record_links(
        &self,
        did_id: u64,
        collection: &str,
        rkey: &str,
        targets: &[(&str, u64)],
    ) -> Result<()> {
        let cf = self.db.cf_handle(CF_LINK_TARGETS).unwrap();
        let opts = bincode_opts();
        let key = RecordLinkKey(
            DidId(did_id),
            Collection(collection.to_string()),
            RKey(rkey.to_string()),
        );
        let value = RecordLinkTargets(
            targets
                .iter()
                .map(|(p, t)| RecordLinkTarget(RPath(p.to_string()), TargetId(*t)))
                .collect(),
        );
        let k = opts.serialize(&key)?;
        let v = opts.serialize(&value)?;
        self.db.put_cf(cf, k, v)?;
        Ok(())
    }

    #[allow(dead_code)]
    pub fn put_target_linkers(&self, target_id: u64, linkers: &[(u64, &str)]) -> Result<()> {
        let cf = self.db.cf_handle(CF_TARGET_LINKS).unwrap();
        let opts = bincode_opts();
        let value = TargetLinkers(
            linkers
                .iter()
                .map(|(d, r)| (DidId(*d), RKey(r.to_string())))
                .collect(),
        );
        let k = opts.serialize(&TargetId(target_id))?;
        let v = opts.serialize(&value)?;
        self.db.put_cf(cf, k, v)?;
        Ok(())
    }

    pub fn close(self) -> Result<()> {
        drop(self.db);
        Ok(())
    }
}

/// Noon UTC of today's date — guaranteed to land in today's UTC day no
/// matter when the test starts.
pub fn today() -> DateTime<Utc> {
    Utc::now()
        .date_naive()
        .and_hms_opt(12, 0, 0)
        .unwrap()
        .and_utc()
}

pub fn yesterday() -> DateTime<Utc> {
    today() - Duration::days(1)
}

pub fn two_days_ago() -> DateTime<Utc> {
    today() - Duration::days(2)
}

//! Transient parquet writers used between rocks scans and Phase 5.
//!
//! Files emitted here live under `<raw_dir>/scratch/` and are deleted
//! at the end of stage. They're written with the same atomic-rename
//! discipline as final outputs so a partial scratch file can never be
//! mistaken for a complete one if the process crashes mid-flight; on
//! the next run we either resume cleanly or re-run scratch from
//! scratch (literal scratch).

use anyhow::{Context, Result};
use arrow_array::builder::{
    StringBuilder, TimestampMicrosecondBuilder, UInt64Builder,
};
use arrow_array::{ArrayRef, RecordBatch};
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use std::path::PathBuf;
use std::sync::Arc;

use super::common::AtomicParquet;

/// Two u64 columns. Used for both `t_post_refs` (target_id, uri_id) and
/// `t_did_refs` (target_id, dst_did_id) — Pass C projection outputs.
/// Naturally sorted by `a` because Pass C iterates target_ids reverse
/// in big-endian u64 order.
pub struct U64PairWriter {
    inner: AtomicParquet,
    schema: Arc<Schema>,
    batch_size: usize,
    rows: usize,
    total: u64,
    a: UInt64Builder,
    b: UInt64Builder,
}

impl U64PairWriter {
    pub fn create(path: PathBuf, batch_size: usize, a_name: &str, b_name: &str) -> Result<Self> {
        let schema = Arc::new(Schema::new(vec![
            Field::new(a_name, DataType::UInt64, false),
            Field::new(b_name, DataType::UInt64, false),
        ]));
        let inner = AtomicParquet::create_scratch(path, schema.clone())?;
        Ok(Self {
            inner,
            schema,
            batch_size,
            rows: 0,
            total: 0,
            a: UInt64Builder::with_capacity(batch_size),
            b: UInt64Builder::with_capacity(batch_size),
        })
    }

    pub fn push(&mut self, a: u64, b: u64) -> Result<()> {
        self.a.append_value(a);
        self.b.append_value(b);
        self.rows += 1;
        if self.rows >= self.batch_size {
            self.flush()?;
        }
        Ok(())
    }

    fn flush(&mut self) -> Result<()> {
        if self.rows == 0 {
            return Ok(());
        }
        let bs = self.batch_size;
        let mut a = std::mem::replace(&mut self.a, UInt64Builder::with_capacity(bs));
        let mut b = std::mem::replace(&mut self.b, UInt64Builder::with_capacity(bs));
        let cols: Vec<ArrayRef> = vec![Arc::new(a.finish()), Arc::new(b.finish())];
        let batch = RecordBatch::try_new(self.schema.clone(), cols).context("u64pair batch")?;
        self.inner.writer.write(&batch).context("write u64pair batch")?;
        self.total += self.rows as u64;
        self.rows = 0;
        Ok(())
    }

    pub fn finish(mut self) -> Result<(PathBuf, u64)> {
        self.flush()?;
        let total = self.total;
        let path = self.inner.finish()?;
        Ok((path, total))
    }
}

/// One row per posts record from Pass B: (post_uri_id, did_id, rkey,
/// created_at). Emitted alongside `LtPostRefsWriter` rows for the
/// per-record reply/quote rpaths. Used in Phase 5 to assemble final
/// posts_from_records rows after refs are resolved.
pub struct LtPostRecordsWriter {
    inner: AtomicParquet,
    schema: Arc<Schema>,
    batch_size: usize,
    rows: usize,
    total: u64,
    post_uri_id: UInt64Builder,
    did_id: UInt64Builder,
    rkey: StringBuilder,
    created_at: TimestampMicrosecondBuilder,
}

impl LtPostRecordsWriter {
    pub fn create(path: PathBuf, batch_size: usize) -> Result<Self> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("post_uri_id", DataType::UInt64, false),
            Field::new("did_id", DataType::UInt64, false),
            Field::new("rkey", DataType::Utf8, false),
            Field::new(
                "created_at",
                DataType::Timestamp(TimeUnit::Microsecond, None),
                true,
            ),
        ]));
        let inner = AtomicParquet::create_scratch(path, schema.clone())?;
        Ok(Self {
            inner,
            schema,
            batch_size,
            rows: 0,
            total: 0,
            post_uri_id: UInt64Builder::with_capacity(batch_size),
            did_id: UInt64Builder::with_capacity(batch_size),
            rkey: StringBuilder::with_capacity(batch_size, batch_size * 16),
            created_at: TimestampMicrosecondBuilder::with_capacity(batch_size),
        })
    }

    pub fn push(
        &mut self,
        post_uri_id: u64,
        did_id: u64,
        rkey: &str,
        created_at: Option<i64>,
    ) -> Result<()> {
        self.post_uri_id.append_value(post_uri_id);
        self.did_id.append_value(did_id);
        self.rkey.append_value(rkey);
        match created_at {
            Some(ts) => self.created_at.append_value(ts),
            None => self.created_at.append_null(),
        }
        self.rows += 1;
        if self.rows >= self.batch_size {
            self.flush()?;
        }
        Ok(())
    }

    fn flush(&mut self) -> Result<()> {
        if self.rows == 0 {
            return Ok(());
        }
        let bs = self.batch_size;
        let mut post = std::mem::replace(&mut self.post_uri_id, UInt64Builder::with_capacity(bs));
        let mut did = std::mem::replace(&mut self.did_id, UInt64Builder::with_capacity(bs));
        let mut rkey =
            std::mem::replace(&mut self.rkey, StringBuilder::with_capacity(bs, bs * 16));
        let mut created_at = std::mem::replace(
            &mut self.created_at,
            TimestampMicrosecondBuilder::with_capacity(bs),
        );
        let cols: Vec<ArrayRef> = vec![
            Arc::new(post.finish()),
            Arc::new(did.finish()),
            Arc::new(rkey.finish()),
            Arc::new(created_at.finish()),
        ];
        let batch = RecordBatch::try_new(self.schema.clone(), cols).context("lt_post_records")?;
        self.inner
            .writer
            .write(&batch)
            .context("write lt_post_records")?;
        self.total += self.rows as u64;
        self.rows = 0;
        Ok(())
    }

    pub fn finish(mut self) -> Result<(PathBuf, u64)> {
        self.flush()?;
        let total = self.total;
        let path = self.inner.finish()?;
        Ok((path, total))
    }
}

/// Per-rpath ref rows for posts: (post_uri_id, rpath, target_id). Only
/// emitted for the four reply/quote rpaths — media rpaths are
/// deliberately dropped (post_media is out of scope; see specs/).
pub struct LtPostRefsWriter {
    inner: AtomicParquet,
    schema: Arc<Schema>,
    batch_size: usize,
    rows: usize,
    total: u64,
    post_uri_id: UInt64Builder,
    rpath: StringBuilder,
    target_id: UInt64Builder,
}

impl LtPostRefsWriter {
    pub fn create(path: PathBuf, batch_size: usize) -> Result<Self> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("post_uri_id", DataType::UInt64, false),
            Field::new("rpath", DataType::Utf8, false),
            Field::new("target_id", DataType::UInt64, false),
        ]));
        let inner = AtomicParquet::create_scratch(path, schema.clone())?;
        Ok(Self {
            inner,
            schema,
            batch_size,
            rows: 0,
            total: 0,
            post_uri_id: UInt64Builder::with_capacity(batch_size),
            rpath: StringBuilder::with_capacity(batch_size, batch_size * 32),
            target_id: UInt64Builder::with_capacity(batch_size),
        })
    }

    pub fn push(&mut self, post_uri_id: u64, rpath: &str, target_id: u64) -> Result<()> {
        self.post_uri_id.append_value(post_uri_id);
        self.rpath.append_value(rpath);
        self.target_id.append_value(target_id);
        self.rows += 1;
        if self.rows >= self.batch_size {
            self.flush()?;
        }
        Ok(())
    }

    fn flush(&mut self) -> Result<()> {
        if self.rows == 0 {
            return Ok(());
        }
        let bs = self.batch_size;
        let mut p = std::mem::replace(&mut self.post_uri_id, UInt64Builder::with_capacity(bs));
        let mut rp = std::mem::replace(
            &mut self.rpath,
            StringBuilder::with_capacity(bs, bs * 32),
        );
        let mut t = std::mem::replace(&mut self.target_id, UInt64Builder::with_capacity(bs));
        let cols: Vec<ArrayRef> = vec![
            Arc::new(p.finish()),
            Arc::new(rp.finish()),
            Arc::new(t.finish()),
        ];
        let batch = RecordBatch::try_new(self.schema.clone(), cols).context("lt_post_refs")?;
        self.inner.writer.write(&batch).context("write lt_post_refs")?;
        self.total += self.rows as u64;
        self.rows = 0;
        Ok(())
    }

    pub fn finish(mut self) -> Result<(PathBuf, u64)> {
        self.flush()?;
        let total = self.total;
        let path = self.inner.finish()?;
        Ok((path, total))
    }
}

/// One scratch row per like / repost / follow / block record from
/// Pass B: (target_id, src_did_id, rkey, created_at). All four
/// collections share this shape — only the file name differs.
/// `target_id` is the .subject(.uri) target the record points at;
/// merge-joining this against the matching Pass C output resolves it
/// to a `dst_did_id` (follows/blocks) or `subject_uri_id`
/// (likes/reposts).
pub struct LtKeyedWriter {
    inner: AtomicParquet,
    schema: Arc<Schema>,
    batch_size: usize,
    rows: usize,
    total: u64,
    target_id: UInt64Builder,
    src_did_id: UInt64Builder,
    rkey: StringBuilder,
    created_at: TimestampMicrosecondBuilder,
}

impl LtKeyedWriter {
    pub fn create(path: PathBuf, batch_size: usize) -> Result<Self> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("target_id", DataType::UInt64, false),
            Field::new("src_did_id", DataType::UInt64, false),
            Field::new("rkey", DataType::Utf8, false),
            Field::new(
                "created_at",
                DataType::Timestamp(TimeUnit::Microsecond, None),
                true,
            ),
        ]));
        let inner = AtomicParquet::create_scratch(path, schema.clone())?;
        Ok(Self {
            inner,
            schema,
            batch_size,
            rows: 0,
            total: 0,
            target_id: UInt64Builder::with_capacity(batch_size),
            src_did_id: UInt64Builder::with_capacity(batch_size),
            rkey: StringBuilder::with_capacity(batch_size, batch_size * 16),
            created_at: TimestampMicrosecondBuilder::with_capacity(batch_size),
        })
    }

    pub fn push(
        &mut self,
        target_id: u64,
        src_did_id: u64,
        rkey: &str,
        created_at: Option<i64>,
    ) -> Result<()> {
        self.target_id.append_value(target_id);
        self.src_did_id.append_value(src_did_id);
        self.rkey.append_value(rkey);
        match created_at {
            Some(ts) => self.created_at.append_value(ts),
            None => self.created_at.append_null(),
        }
        self.rows += 1;
        if self.rows >= self.batch_size {
            self.flush()?;
        }
        Ok(())
    }

    fn flush(&mut self) -> Result<()> {
        if self.rows == 0 {
            return Ok(());
        }
        let bs = self.batch_size;
        let mut t = std::mem::replace(&mut self.target_id, UInt64Builder::with_capacity(bs));
        let mut s = std::mem::replace(&mut self.src_did_id, UInt64Builder::with_capacity(bs));
        let mut rk =
            std::mem::replace(&mut self.rkey, StringBuilder::with_capacity(bs, bs * 16));
        let mut ca = std::mem::replace(
            &mut self.created_at,
            TimestampMicrosecondBuilder::with_capacity(bs),
        );
        let cols: Vec<ArrayRef> = vec![
            Arc::new(t.finish()),
            Arc::new(s.finish()),
            Arc::new(rk.finish()),
            Arc::new(ca.finish()),
        ];
        let batch = RecordBatch::try_new(self.schema.clone(), cols).context("lt_keyed batch")?;
        self.inner.writer.write(&batch).context("write lt_keyed")?;
        self.total += self.rows as u64;
        self.rows = 0;
        Ok(())
    }

    pub fn finish(mut self) -> Result<(PathBuf, u64)> {
        self.flush()?;
        let total = self.total;
        let path = self.inner.finish()?;
        Ok((path, total))
    }
}

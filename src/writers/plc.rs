use anyhow::{Context, Result};
use arrow_array::builder::{StringBuilder, TimestampMillisecondBuilder};
use arrow_array::{ArrayRef, RecordBatch};
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use std::path::PathBuf;
use std::sync::Arc;

use super::common::AtomicParquet;

/// Writer for the PLC export shards consumed by
/// `07_enrich_actors_created.sql`. One row per captured PLC operation:
/// `(did, kind, ts, pds, handle)` where `kind` is `"create"` (genesis op),
/// `"tombstone"` (plc_tombstone op), or `"update"` (later plc_operation —
/// migration/rotation, which can change the PDS). `ts` is the op's `createdAt`
/// (epoch milliseconds, UTC, no timezone — matches DuckDB TIMESTAMP). `pds` is
/// the atproto PDS endpoint at that op (NULL for tombstones); `handle` the
/// account handle at that op.
pub struct PlcOpWriter {
    inner: AtomicParquet,
    schema: Arc<Schema>,
    batch_size: usize,
    rows: usize,
    total: u64,
    did: StringBuilder,
    kind: StringBuilder,
    ts: TimestampMillisecondBuilder,
    pds: StringBuilder,
    handle: StringBuilder,
}

impl PlcOpWriter {
    pub fn create(path: PathBuf, batch_size: usize) -> Result<Self> {
        let schema = Self::schema();
        let inner = AtomicParquet::create(path, schema.clone())?;
        Ok(PlcOpWriter {
            inner,
            schema,
            batch_size,
            rows: 0,
            total: 0,
            did: StringBuilder::with_capacity(batch_size, batch_size * 32),
            kind: StringBuilder::with_capacity(batch_size, batch_size * 8),
            ts: TimestampMillisecondBuilder::with_capacity(batch_size),
            pds: StringBuilder::with_capacity(batch_size, batch_size * 40),
            handle: StringBuilder::with_capacity(batch_size, batch_size * 24),
        })
    }

    fn schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("did", DataType::Utf8, false),
            Field::new("kind", DataType::Utf8, false),
            Field::new("ts", DataType::Timestamp(TimeUnit::Millisecond, None), true),
            Field::new("pds", DataType::Utf8, true),
            Field::new("handle", DataType::Utf8, true),
        ]))
    }

    /// Append one op. `ts_ms` is epoch-millis or None if the op's
    /// createdAt was unparseable. `pds`/`handle` are None when the op
    /// doesn't carry them (e.g. tombstones have no PDS).
    pub fn push(
        &mut self,
        did: &str,
        kind: &str,
        ts_ms: Option<i64>,
        pds: Option<&str>,
        handle: Option<&str>,
    ) -> Result<()> {
        self.did.append_value(did);
        self.kind.append_value(kind);
        self.ts.append_option(ts_ms);
        self.pds.append_option(pds);
        self.handle.append_option(handle);
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
        let mut did = std::mem::replace(&mut self.did, StringBuilder::with_capacity(bs, bs * 32));
        let mut kind = std::mem::replace(&mut self.kind, StringBuilder::with_capacity(bs, bs * 8));
        let mut ts = std::mem::replace(&mut self.ts, TimestampMillisecondBuilder::with_capacity(bs));
        let mut pds = std::mem::replace(&mut self.pds, StringBuilder::with_capacity(bs, bs * 40));
        let mut handle =
            std::mem::replace(&mut self.handle, StringBuilder::with_capacity(bs, bs * 24));
        let cols: Vec<ArrayRef> = vec![
            Arc::new(did.finish()),
            Arc::new(kind.finish()),
            Arc::new(ts.finish()),
            Arc::new(pds.finish()),
            Arc::new(handle.finish()),
        ];
        let batch = RecordBatch::try_new(self.schema.clone(), cols).context("plc batch")?;
        self.inner.writer.write(&batch).context("write plc batch")?;
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

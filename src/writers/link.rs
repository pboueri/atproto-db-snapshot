use anyhow::{Context, Result};
use arrow_array::builder::{
    Int32Builder, StringBuilder, TimestampMicrosecondBuilder, UInt64Builder,
};
use arrow_array::{ArrayRef, RecordBatch};
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use std::path::PathBuf;
use std::sync::Arc;

use super::common::AtomicParquet;

/// One row per RecordLinkKey: identifies a record by (did_id, collection,
/// rkey) and pre-computes its TID-decoded `created_at`. Unresolved — the
/// record's targets live in `link_record_targets.parquet`.
pub struct LinkRecordWriter {
    inner: AtomicParquet,
    schema: Arc<Schema>,
    batch_size: usize,
    rows: usize,
    total: u64,
    did_id: UInt64Builder,
    collection: StringBuilder,
    rkey: StringBuilder,
    created_at: TimestampMicrosecondBuilder,
}

impl LinkRecordWriter {
    pub fn create(path: PathBuf, batch_size: usize) -> Result<Self> {
        let schema = Self::schema();
        let inner = AtomicParquet::create(path, schema.clone())?;
        Ok(Self {
            inner,
            schema,
            batch_size,
            rows: 0,
            total: 0,
            did_id: UInt64Builder::with_capacity(batch_size),
            collection: StringBuilder::with_capacity(batch_size, batch_size * 24),
            rkey: StringBuilder::with_capacity(batch_size, batch_size * 16),
            created_at: TimestampMicrosecondBuilder::with_capacity(batch_size),
        })
    }

    fn schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("did_id", DataType::UInt64, false),
            Field::new("collection", DataType::Utf8, false),
            Field::new("rkey", DataType::Utf8, false),
            Field::new(
                "created_at",
                DataType::Timestamp(TimeUnit::Microsecond, None),
                true,
            ),
        ]))
    }

    pub fn push(
        &mut self,
        did_id: u64,
        collection: &str,
        rkey: &str,
        created_at: Option<i64>,
    ) -> Result<()> {
        self.did_id.append_value(did_id);
        self.collection.append_value(collection);
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
        let mut did_id =
            std::mem::replace(&mut self.did_id, UInt64Builder::with_capacity(bs));
        let mut collection = std::mem::replace(
            &mut self.collection,
            StringBuilder::with_capacity(bs, bs * 24),
        );
        let mut rkey =
            std::mem::replace(&mut self.rkey, StringBuilder::with_capacity(bs, bs * 16));
        let mut created_at = std::mem::replace(
            &mut self.created_at,
            TimestampMicrosecondBuilder::with_capacity(bs),
        );
        let cols: Vec<ArrayRef> = vec![
            Arc::new(did_id.finish()),
            Arc::new(collection.finish()),
            Arc::new(rkey.finish()),
            Arc::new(created_at.finish()),
        ];
        let batch =
            RecordBatch::try_new(self.schema.clone(), cols).context("link_records batch")?;
        self.inner
            .writer
            .write(&batch)
            .context("write link_records batch")?;
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

/// One row per (RecordLinkKey, rpath, target_id) triple. `ord` is the
/// 0-based position of the target within the record's targets vec, used to
/// preserve ordering of repeated rpaths (e.g. `.embed.images[0..N]`).
pub struct LinkRecordTargetsWriter {
    inner: AtomicParquet,
    schema: Arc<Schema>,
    batch_size: usize,
    rows: usize,
    total: u64,
    did_id: UInt64Builder,
    collection: StringBuilder,
    rkey: StringBuilder,
    ord: Int32Builder,
    rpath: StringBuilder,
    target_id: UInt64Builder,
}

impl LinkRecordTargetsWriter {
    pub fn create(path: PathBuf, batch_size: usize) -> Result<Self> {
        let schema = Self::schema();
        let inner = AtomicParquet::create(path, schema.clone())?;
        Ok(Self {
            inner,
            schema,
            batch_size,
            rows: 0,
            total: 0,
            did_id: UInt64Builder::with_capacity(batch_size),
            collection: StringBuilder::with_capacity(batch_size, batch_size * 24),
            rkey: StringBuilder::with_capacity(batch_size, batch_size * 16),
            ord: Int32Builder::with_capacity(batch_size),
            rpath: StringBuilder::with_capacity(batch_size, batch_size * 24),
            target_id: UInt64Builder::with_capacity(batch_size),
        })
    }

    fn schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("did_id", DataType::UInt64, false),
            Field::new("collection", DataType::Utf8, false),
            Field::new("rkey", DataType::Utf8, false),
            Field::new("ord", DataType::Int32, false),
            Field::new("rpath", DataType::Utf8, false),
            Field::new("target_id", DataType::UInt64, false),
        ]))
    }

    pub fn push(
        &mut self,
        did_id: u64,
        collection: &str,
        rkey: &str,
        ord: i32,
        rpath: &str,
        target_id: u64,
    ) -> Result<()> {
        self.did_id.append_value(did_id);
        self.collection.append_value(collection);
        self.rkey.append_value(rkey);
        self.ord.append_value(ord);
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
        let mut did_id =
            std::mem::replace(&mut self.did_id, UInt64Builder::with_capacity(bs));
        let mut collection = std::mem::replace(
            &mut self.collection,
            StringBuilder::with_capacity(bs, bs * 24),
        );
        let mut rkey =
            std::mem::replace(&mut self.rkey, StringBuilder::with_capacity(bs, bs * 16));
        let mut ord = std::mem::replace(&mut self.ord, Int32Builder::with_capacity(bs));
        let mut rpath =
            std::mem::replace(&mut self.rpath, StringBuilder::with_capacity(bs, bs * 24));
        let mut target_id =
            std::mem::replace(&mut self.target_id, UInt64Builder::with_capacity(bs));
        let cols: Vec<ArrayRef> = vec![
            Arc::new(did_id.finish()),
            Arc::new(collection.finish()),
            Arc::new(rkey.finish()),
            Arc::new(ord.finish()),
            Arc::new(rpath.finish()),
            Arc::new(target_id.finish()),
        ];
        let batch = RecordBatch::try_new(self.schema.clone(), cols)
            .context("link_record_targets batch")?;
        self.inner
            .writer
            .write(&batch)
            .context("write link_record_targets batch")?;
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

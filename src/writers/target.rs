use anyhow::{Context, Result};
use arrow_array::builder::{StringBuilder, UInt64Builder};
use arrow_array::{ArrayRef, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use std::path::PathBuf;
use std::sync::Arc;

use super::common::AtomicParquet;

/// Full dump of the `target_ids` forward map: one row per unique
/// (target, collection, rpath) triple along with its assigned id. Hydrate
/// joins this against `link_record_targets` on `target_id` to resolve
/// target URIs without doing point lookups during the link_targets scan.
pub struct TargetWriter {
    inner: AtomicParquet,
    schema: Arc<Schema>,
    batch_size: usize,
    rows: usize,
    total: u64,
    target_id: UInt64Builder,
    target: StringBuilder,
    target_collection: StringBuilder,
    target_rpath: StringBuilder,
}

impl TargetWriter {
    pub fn create(path: PathBuf, batch_size: usize) -> Result<Self> {
        let schema = Self::schema();
        let inner = AtomicParquet::create(path, schema.clone())?;
        Ok(Self {
            inner,
            schema,
            batch_size,
            rows: 0,
            total: 0,
            target_id: UInt64Builder::with_capacity(batch_size),
            target: StringBuilder::with_capacity(batch_size, batch_size * 80),
            target_collection: StringBuilder::with_capacity(batch_size, batch_size * 24),
            target_rpath: StringBuilder::with_capacity(batch_size, batch_size * 24),
        })
    }

    fn schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("target_id", DataType::UInt64, false),
            Field::new("target", DataType::Utf8, false),
            Field::new("target_collection", DataType::Utf8, false),
            Field::new("target_rpath", DataType::Utf8, false),
        ]))
    }

    pub fn push(
        &mut self,
        target_id: u64,
        target: &str,
        target_collection: &str,
        target_rpath: &str,
    ) -> Result<()> {
        self.target_id.append_value(target_id);
        self.target.append_value(target);
        self.target_collection.append_value(target_collection);
        self.target_rpath.append_value(target_rpath);
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
        let mut target_id =
            std::mem::replace(&mut self.target_id, UInt64Builder::with_capacity(bs));
        let mut target =
            std::mem::replace(&mut self.target, StringBuilder::with_capacity(bs, bs * 80));
        let mut target_collection = std::mem::replace(
            &mut self.target_collection,
            StringBuilder::with_capacity(bs, bs * 24),
        );
        let mut target_rpath = std::mem::replace(
            &mut self.target_rpath,
            StringBuilder::with_capacity(bs, bs * 24),
        );
        let cols: Vec<ArrayRef> = vec![
            Arc::new(target_id.finish()),
            Arc::new(target.finish()),
            Arc::new(target_collection.finish()),
            Arc::new(target_rpath.finish()),
        ];
        let batch =
            RecordBatch::try_new(self.schema.clone(), cols).context("targets batch")?;
        self.inner
            .writer
            .write(&batch)
            .context("write targets batch")?;
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

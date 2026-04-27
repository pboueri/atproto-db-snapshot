use anyhow::{Context, Result};
use arrow_array::builder::{BooleanBuilder, StringBuilder, UInt64Builder};
use arrow_array::{ArrayRef, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use std::path::PathBuf;
use std::sync::Arc;

use super::common::AtomicParquet;

pub struct ActorWriter {
    inner: AtomicParquet,
    batch_size: usize,
    rows: usize,
    total: u64,
    did_id: UInt64Builder,
    did: StringBuilder,
    active: BooleanBuilder,
}

impl ActorWriter {
    pub fn create(path: PathBuf, batch_size: usize) -> Result<Self> {
        let schema = Self::schema();
        let inner = AtomicParquet::create(path, schema)?;
        Ok(ActorWriter {
            inner,
            batch_size,
            rows: 0,
            total: 0,
            did_id: UInt64Builder::with_capacity(batch_size),
            did: StringBuilder::with_capacity(batch_size, batch_size * 32),
            active: BooleanBuilder::with_capacity(batch_size),
        })
    }

    fn schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("did_id", DataType::UInt64, false),
            Field::new("did", DataType::Utf8, false),
            Field::new("active", DataType::Boolean, false),
        ]))
    }

    pub fn push(&mut self, did_id: u64, did: &str, active: bool) -> Result<()> {
        self.did_id.append_value(did_id);
        self.did.append_value(did);
        self.active.append_value(active);
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
        let cols: Vec<ArrayRef> = vec![
            Arc::new(self.did_id.finish()),
            Arc::new(self.did.finish()),
            Arc::new(self.active.finish()),
        ];
        let batch = RecordBatch::try_new(Self::schema(), cols).context("actor batch")?;
        self.inner.writer.write(&batch).context("write actor batch")?;
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

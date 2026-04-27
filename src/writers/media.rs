use anyhow::{Context, Result};
use arrow_array::builder::{Int32Builder, StringBuilder};
use arrow_array::{ArrayRef, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use std::path::PathBuf;
use std::sync::Arc;

use super::common::AtomicParquet;

pub struct PostMediaWriter {
    inner: AtomicParquet,
    batch_size: usize,
    rows: usize,
    total: u64,
    uri: StringBuilder,
    ord: Int32Builder,
    kind: StringBuilder,
    reference: StringBuilder,
}

impl PostMediaWriter {
    pub fn create(path: PathBuf, batch_size: usize) -> Result<Self> {
        let schema = Self::schema();
        let inner = AtomicParquet::create(path, schema)?;
        Ok(Self {
            inner,
            batch_size,
            rows: 0,
            total: 0,
            uri: StringBuilder::with_capacity(batch_size, batch_size * 80),
            ord: Int32Builder::with_capacity(batch_size),
            kind: StringBuilder::with_capacity(batch_size, batch_size * 8),
            reference: StringBuilder::with_capacity(batch_size, batch_size * 64),
        })
    }

    fn schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("uri", DataType::Utf8, false),
            Field::new("ord", DataType::Int32, false),
            Field::new("kind", DataType::Utf8, false),
            Field::new("ref", DataType::Utf8, false),
        ]))
    }

    pub fn push(&mut self, uri: &str, ord: i32, kind: &str, reference: &str) -> Result<()> {
        self.uri.append_value(uri);
        self.ord.append_value(ord);
        self.kind.append_value(kind);
        self.reference.append_value(reference);
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
            Arc::new(self.uri.finish()),
            Arc::new(self.ord.finish()),
            Arc::new(self.kind.finish()),
            Arc::new(self.reference.finish()),
        ];
        let batch = RecordBatch::try_new(Self::schema(), cols).context("post media batch")?;
        self.inner
            .writer
            .write(&batch)
            .context("write post media batch")?;
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

use anyhow::{Context, Result};
use arrow_array::builder::{StringBuilder, TimestampMicrosecondBuilder};
use arrow_array::{ArrayRef, RecordBatch};
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use std::path::PathBuf;
use std::sync::Arc;

use super::common::AtomicParquet;

pub struct PostFromTargetWriter {
    inner: AtomicParquet,
    schema: Arc<Schema>,
    batch_size: usize,
    rows: usize,
    total: u64,
    uri: StringBuilder,
    author_did: StringBuilder,
    rkey: StringBuilder,
    created_at: TimestampMicrosecondBuilder,
}

impl PostFromTargetWriter {
    pub fn create(path: PathBuf, batch_size: usize) -> Result<Self> {
        let schema = Self::schema();
        let inner = AtomicParquet::create(path, schema.clone())?;
        Ok(Self {
            inner,
            schema,
            batch_size,
            rows: 0,
            total: 0,
            uri: StringBuilder::with_capacity(batch_size, batch_size * 80),
            author_did: StringBuilder::with_capacity(batch_size, batch_size * 32),
            rkey: StringBuilder::with_capacity(batch_size, batch_size * 16),
            created_at: TimestampMicrosecondBuilder::with_capacity(batch_size),
        })
    }

    fn schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("uri", DataType::Utf8, false),
            Field::new("author_did", DataType::Utf8, false),
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
        uri: &str,
        author_did: &str,
        rkey: &str,
        created_at: Option<i64>,
    ) -> Result<()> {
        self.uri.append_value(uri);
        self.author_did.append_value(author_did);
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
        let mut uri =
            std::mem::replace(&mut self.uri, StringBuilder::with_capacity(bs, bs * 80));
        let mut author_did = std::mem::replace(
            &mut self.author_did,
            StringBuilder::with_capacity(bs, bs * 32),
        );
        let mut rkey =
            std::mem::replace(&mut self.rkey, StringBuilder::with_capacity(bs, bs * 16));
        let mut created_at = std::mem::replace(
            &mut self.created_at,
            TimestampMicrosecondBuilder::with_capacity(bs),
        );
        let cols: Vec<ArrayRef> = vec![
            Arc::new(uri.finish()),
            Arc::new(author_did.finish()),
            Arc::new(rkey.finish()),
            Arc::new(created_at.finish()),
        ];
        let batch =
            RecordBatch::try_new(self.schema.clone(), cols).context("post-from-target batch")?;
        self.inner
            .writer
            .write(&batch)
            .context("write post-from-target batch")?;
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

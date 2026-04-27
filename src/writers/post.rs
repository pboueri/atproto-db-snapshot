use anyhow::{Context, Result};
use arrow_array::builder::{StringBuilder, TimestampMicrosecondBuilder, UInt64Builder};
use arrow_array::{ArrayRef, RecordBatch};
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use std::path::PathBuf;
use std::sync::Arc;

use super::common::AtomicParquet;

pub struct PostFromRecordWriter {
    inner: AtomicParquet,
    batch_size: usize,
    rows: usize,
    total: u64,
    uri: StringBuilder,
    author_did_id: UInt64Builder,
    rkey: StringBuilder,
    created_at: TimestampMicrosecondBuilder,
    reply_root_uri: StringBuilder,
    reply_parent_uri: StringBuilder,
    quote_uri: StringBuilder,
}

impl PostFromRecordWriter {
    pub fn create(path: PathBuf, batch_size: usize) -> Result<Self> {
        let schema = Self::schema();
        let inner = AtomicParquet::create(path, schema)?;
        Ok(Self {
            inner,
            batch_size,
            rows: 0,
            total: 0,
            uri: StringBuilder::with_capacity(batch_size, batch_size * 80),
            author_did_id: UInt64Builder::with_capacity(batch_size),
            rkey: StringBuilder::with_capacity(batch_size, batch_size * 16),
            created_at: TimestampMicrosecondBuilder::with_capacity(batch_size),
            reply_root_uri: StringBuilder::with_capacity(batch_size, batch_size * 16),
            reply_parent_uri: StringBuilder::with_capacity(batch_size, batch_size * 16),
            quote_uri: StringBuilder::with_capacity(batch_size, batch_size * 16),
        })
    }

    fn schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("uri", DataType::Utf8, false),
            Field::new("author_did_id", DataType::UInt64, false),
            Field::new("rkey", DataType::Utf8, false),
            Field::new(
                "created_at",
                DataType::Timestamp(TimeUnit::Microsecond, None),
                true,
            ),
            Field::new("reply_root_uri", DataType::Utf8, true),
            Field::new("reply_parent_uri", DataType::Utf8, true),
            Field::new("quote_uri", DataType::Utf8, true),
        ]))
    }

    #[allow(clippy::too_many_arguments)]
    pub fn push(
        &mut self,
        uri: &str,
        author_did_id: u64,
        rkey: &str,
        created_at: Option<i64>,
        reply_root_uri: Option<&str>,
        reply_parent_uri: Option<&str>,
        quote_uri: Option<&str>,
    ) -> Result<()> {
        self.uri.append_value(uri);
        self.author_did_id.append_value(author_did_id);
        self.rkey.append_value(rkey);
        match created_at {
            Some(ts) => self.created_at.append_value(ts),
            None => self.created_at.append_null(),
        }
        match reply_root_uri {
            Some(s) => self.reply_root_uri.append_value(s),
            None => self.reply_root_uri.append_null(),
        }
        match reply_parent_uri {
            Some(s) => self.reply_parent_uri.append_value(s),
            None => self.reply_parent_uri.append_null(),
        }
        match quote_uri {
            Some(s) => self.quote_uri.append_value(s),
            None => self.quote_uri.append_null(),
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
        let cols: Vec<ArrayRef> = vec![
            Arc::new(self.uri.finish()),
            Arc::new(self.author_did_id.finish()),
            Arc::new(self.rkey.finish()),
            Arc::new(self.created_at.finish()),
            Arc::new(self.reply_root_uri.finish()),
            Arc::new(self.reply_parent_uri.finish()),
            Arc::new(self.quote_uri.finish()),
        ];
        let batch = RecordBatch::try_new(Self::schema(), cols).context("post-from-record batch")?;
        self.inner
            .writer
            .write(&batch)
            .context("write post-from-record batch")?;
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

pub struct PostFromTargetWriter {
    inner: AtomicParquet,
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
        let inner = AtomicParquet::create(path, schema)?;
        Ok(Self {
            inner,
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
        let cols: Vec<ArrayRef> = vec![
            Arc::new(self.uri.finish()),
            Arc::new(self.author_did.finish()),
            Arc::new(self.rkey.finish()),
            Arc::new(self.created_at.finish()),
        ];
        let batch = RecordBatch::try_new(Self::schema(), cols).context("post-from-target batch")?;
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

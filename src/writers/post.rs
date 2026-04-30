//! Writer for `posts.parquet`. Posts have the most fields because
//! reply/quote target URIs are pre-resolved to `*_uri_id`s during
//! the rocks scan — downstream tables like `post_aggs` join on
//! these without any URI string operations.
use anyhow::{Context, Result};
use arrow_array::builder::{StringBuilder, TimestampMicrosecondBuilder, UInt64Builder};
use arrow_array::{ArrayRef, RecordBatch};
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use std::path::PathBuf;
use std::sync::Arc;

use super::common::AtomicParquet;

pub struct PostsWriter {
    inner: AtomicParquet,
    schema: Arc<Schema>,
    batch_size: usize,
    rows: usize,
    total: u64,
    uri_id: UInt64Builder,
    author_did_id: UInt64Builder,
    rkey: StringBuilder,
    created_at: TimestampMicrosecondBuilder,
    reply_root_uri_id: UInt64Builder,
    reply_parent_uri_id: UInt64Builder,
    quote_uri_id: UInt64Builder,
}

impl PostsWriter {
    pub fn create(path: PathBuf, batch_size: usize) -> Result<Self> {
        Self::create_inner(path, batch_size, false)
    }

    /// LZ4-compressed transient variant. Used by Pass C for the
    /// posts_from_targets_raw scratch file that gets re-COPYed to a
    /// ZSTD final by Phase 5 and then deleted.
    pub fn create_scratch(path: PathBuf, batch_size: usize) -> Result<Self> {
        Self::create_inner(path, batch_size, true)
    }

    fn create_inner(path: PathBuf, batch_size: usize, scratch: bool) -> Result<Self> {
        let schema = Self::schema();
        let inner = if scratch {
            AtomicParquet::create_scratch(path, schema.clone())?
        } else {
            AtomicParquet::create(path, schema.clone())?
        };
        Ok(Self {
            inner,
            schema,
            batch_size,
            rows: 0,
            total: 0,
            uri_id: UInt64Builder::with_capacity(batch_size),
            author_did_id: UInt64Builder::with_capacity(batch_size),
            rkey: StringBuilder::with_capacity(batch_size, batch_size * 16),
            created_at: TimestampMicrosecondBuilder::with_capacity(batch_size),
            reply_root_uri_id: UInt64Builder::with_capacity(batch_size),
            reply_parent_uri_id: UInt64Builder::with_capacity(batch_size),
            quote_uri_id: UInt64Builder::with_capacity(batch_size),
        })
    }

    fn schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("uri_id", DataType::UInt64, false),
            Field::new("author_did_id", DataType::UInt64, false),
            Field::new("rkey", DataType::Utf8, false),
            Field::new(
                "created_at",
                DataType::Timestamp(TimeUnit::Microsecond, None),
                true,
            ),
            Field::new("reply_root_uri_id", DataType::UInt64, true),
            Field::new("reply_parent_uri_id", DataType::UInt64, true),
            Field::new("quote_uri_id", DataType::UInt64, true),
        ]))
    }

    pub fn push(
        &mut self,
        uri_id: u64,
        author_did_id: u64,
        rkey: &str,
        created_at: Option<i64>,
        reply_root_uri_id: Option<u64>,
        reply_parent_uri_id: Option<u64>,
        quote_uri_id: Option<u64>,
    ) -> Result<()> {
        self.uri_id.append_value(uri_id);
        self.author_did_id.append_value(author_did_id);
        self.rkey.append_value(rkey);
        match created_at {
            Some(ts) => self.created_at.append_value(ts),
            None => self.created_at.append_null(),
        }
        self.reply_root_uri_id.append_option(reply_root_uri_id);
        self.reply_parent_uri_id.append_option(reply_parent_uri_id);
        self.quote_uri_id.append_option(quote_uri_id);
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
        let mut uri_id = std::mem::replace(&mut self.uri_id, UInt64Builder::with_capacity(bs));
        let mut author = std::mem::replace(
            &mut self.author_did_id,
            UInt64Builder::with_capacity(bs),
        );
        let mut rkey =
            std::mem::replace(&mut self.rkey, StringBuilder::with_capacity(bs, bs * 16));
        let mut created_at = std::mem::replace(
            &mut self.created_at,
            TimestampMicrosecondBuilder::with_capacity(bs),
        );
        let mut rr = std::mem::replace(
            &mut self.reply_root_uri_id,
            UInt64Builder::with_capacity(bs),
        );
        let mut rp = std::mem::replace(
            &mut self.reply_parent_uri_id,
            UInt64Builder::with_capacity(bs),
        );
        let mut q = std::mem::replace(
            &mut self.quote_uri_id,
            UInt64Builder::with_capacity(bs),
        );
        let cols: Vec<ArrayRef> = vec![
            Arc::new(uri_id.finish()),
            Arc::new(author.finish()),
            Arc::new(rkey.finish()),
            Arc::new(created_at.finish()),
            Arc::new(rr.finish()),
            Arc::new(rp.finish()),
            Arc::new(q.finish()),
        ];
        let batch = RecordBatch::try_new(self.schema.clone(), cols).context("posts batch")?;
        self.inner
            .writer
            .write(&batch)
            .context("write posts batch")?;
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

use anyhow::{Context, Result};
use arrow_array::builder::{StringBuilder, TimestampMicrosecondBuilder, UInt64Builder};
use arrow_array::{ArrayRef, RecordBatch};
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use std::path::PathBuf;
use std::sync::Arc;

use super::common::AtomicParquet;

pub type FollowWriter = ActorActorEdgeWriter;
pub type BlockWriter = ActorActorEdgeWriter;
pub type LikeWriter = ActorSubjectEdgeWriter;
pub type RepostWriter = ActorSubjectEdgeWriter;

pub struct ActorActorEdgeWriter {
    inner: AtomicParquet,
    batch_size: usize,
    rows: usize,
    total: u64,
    src: UInt64Builder,
    dst: UInt64Builder,
    rkey: StringBuilder,
    created_at: TimestampMicrosecondBuilder,
    src_name: &'static str,
    dst_name: &'static str,
}

impl ActorActorEdgeWriter {
    pub fn create(
        path: PathBuf,
        batch_size: usize,
        src_name: &'static str,
        dst_name: &'static str,
    ) -> Result<Self> {
        let schema = Self::schema(src_name, dst_name);
        let inner = AtomicParquet::create(path, schema)?;
        Ok(Self {
            inner,
            batch_size,
            rows: 0,
            total: 0,
            src: UInt64Builder::with_capacity(batch_size),
            dst: UInt64Builder::with_capacity(batch_size),
            rkey: StringBuilder::with_capacity(batch_size, batch_size * 16),
            created_at: TimestampMicrosecondBuilder::with_capacity(batch_size),
            src_name,
            dst_name,
        })
    }

    fn schema(src_name: &str, dst_name: &str) -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new(src_name, DataType::UInt64, false),
            Field::new(dst_name, DataType::UInt64, false),
            Field::new("rkey", DataType::Utf8, false),
            Field::new(
                "created_at",
                DataType::Timestamp(TimeUnit::Microsecond, None),
                true,
            ),
        ]))
    }

    pub fn push(&mut self, src: u64, dst: u64, rkey: &str, created_at: Option<i64>) -> Result<()> {
        self.src.append_value(src);
        self.dst.append_value(dst);
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
            Arc::new(self.src.finish()),
            Arc::new(self.dst.finish()),
            Arc::new(self.rkey.finish()),
            Arc::new(self.created_at.finish()),
        ];
        let batch = RecordBatch::try_new(Self::schema(self.src_name, self.dst_name), cols)
            .context("edge batch")?;
        self.inner.writer.write(&batch).context("write edge batch")?;
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

pub struct ActorSubjectEdgeWriter {
    inner: AtomicParquet,
    batch_size: usize,
    rows: usize,
    total: u64,
    actor: UInt64Builder,
    subject_uri: StringBuilder,
    rkey: StringBuilder,
    created_at: TimestampMicrosecondBuilder,
}

impl ActorSubjectEdgeWriter {
    pub fn create(path: PathBuf, batch_size: usize) -> Result<Self> {
        let schema = Self::schema();
        let inner = AtomicParquet::create(path, schema)?;
        Ok(Self {
            inner,
            batch_size,
            rows: 0,
            total: 0,
            actor: UInt64Builder::with_capacity(batch_size),
            subject_uri: StringBuilder::with_capacity(batch_size, batch_size * 64),
            rkey: StringBuilder::with_capacity(batch_size, batch_size * 16),
            created_at: TimestampMicrosecondBuilder::with_capacity(batch_size),
        })
    }

    fn schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("actor_did_id", DataType::UInt64, false),
            Field::new("subject_uri", DataType::Utf8, false),
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
        actor: u64,
        subject_uri: &str,
        rkey: &str,
        created_at: Option<i64>,
    ) -> Result<()> {
        self.actor.append_value(actor);
        self.subject_uri.append_value(subject_uri);
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
            Arc::new(self.actor.finish()),
            Arc::new(self.subject_uri.finish()),
            Arc::new(self.rkey.finish()),
            Arc::new(self.created_at.finish()),
        ];
        let batch = RecordBatch::try_new(Self::schema(), cols).context("subject edge batch")?;
        self.inner
            .writer
            .write(&batch)
            .context("write subject edge batch")?;
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

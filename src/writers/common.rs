use anyhow::{Context, Result};
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties;
use std::fs::File;
use std::path::{Path, PathBuf};

pub fn writer_props() -> WriterProperties {
    WriterProperties::builder()
        .set_compression(Compression::ZSTD(ZstdLevel::try_new(3).unwrap()))
        .set_max_row_group_row_count(Some(1_000_000))
        .build()
}

pub struct AtomicParquet {
    pub final_path: PathBuf,
    pub tmp_path: PathBuf,
    pub writer: ArrowWriter<File>,
}

impl AtomicParquet {
    pub fn create(path: PathBuf, schema: arrow_schema::SchemaRef) -> Result<Self> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("create parent {}", parent.display()))?;
        }
        let tmp_path = path.with_extension("parquet.tmp");
        let file = File::create(&tmp_path)
            .with_context(|| format!("create {}", tmp_path.display()))?;
        let writer = ArrowWriter::try_new(file, schema, Some(writer_props()))
            .context("create ArrowWriter")?;
        Ok(AtomicParquet {
            final_path: path,
            tmp_path,
            writer,
        })
    }

    pub fn finish(self) -> Result<PathBuf> {
        self.writer.close().context("close ArrowWriter")?;
        std::fs::rename(&self.tmp_path, &self.final_path).with_context(|| {
            format!(
                "rename {} -> {}",
                self.tmp_path.display(),
                self.final_path.display()
            )
        })?;
        Ok(self.final_path)
    }
}

pub fn ensure_dir(path: &Path) -> Result<()> {
    std::fs::create_dir_all(path).with_context(|| format!("mkdir -p {}", path.display()))
}

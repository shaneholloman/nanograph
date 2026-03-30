use std::path::Path;
use std::sync::Arc;

use arrow_array::{RecordBatch, RecordBatchIterator};
use async_trait::async_trait;
use futures::StreamExt;
use lance::Dataset;
use lance::dataset::{MergeInsertBuilder, WhenMatched, WhenNotMatched, WriteMode, WriteParams};
use lance_file::version::LanceFileVersion;
use tracing::info;

use crate::error::{NanoError, Result};
use crate::store::graph_types::{GraphTableId, GraphTableVersion};

pub(crate) const LANCE_INTERNAL_ID_FIELD: &str = "__ng_id";
pub(crate) const LANCE_INTERNAL_SRC_FIELD: &str = "__ng_src";
pub(crate) const LANCE_INTERNAL_DST_FIELD: &str = "__ng_dst";
const DEFAULT_NEW_DATASET_STORAGE_VERSION: LanceFileVersion = LanceFileVersion::V2_2;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LanceDatasetKind {
    Node,
    Edge,
    Plain,
}

pub(crate) fn logical_node_field_to_lance(field_name: &str) -> &str {
    match field_name {
        "id" => LANCE_INTERNAL_ID_FIELD,
        other => other,
    }
}

pub(crate) fn graph_table_id_for_path(path: &Path) -> GraphTableId {
    GraphTableId(path.to_string_lossy().to_string())
}

#[async_trait]
pub(crate) trait TableStore {
    async fn overwrite(&self, path: &Path, batch: RecordBatch) -> Result<GraphTableVersion>;
    async fn append(&self, path: &Path, batch: RecordBatch) -> Result<GraphTableVersion>;
    async fn merge_insert_with_key(
        &self,
        path: &Path,
        pinned_version: &GraphTableVersion,
        source_batch: RecordBatch,
        key_prop: &str,
    ) -> Result<GraphTableVersion>;
    async fn delete_by_ids(
        &self,
        path: &Path,
        pinned_version: &GraphTableVersion,
        ids: &[u64],
    ) -> Result<GraphTableVersion>;
    async fn latest_version(&self, path: &Path) -> Result<GraphTableVersion>;
    async fn read_all_batches(&self, version: &GraphTableVersion) -> Result<Vec<RecordBatch>>;
}

#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct V3TableStore;

impl V3TableStore {
    pub(crate) fn new() -> Self {
        Self
    }
}

pub(crate) fn logical_edge_field_to_lance(field_name: &str) -> &str {
    match field_name {
        "id" => LANCE_INTERNAL_ID_FIELD,
        "src" => LANCE_INTERNAL_SRC_FIELD,
        "dst" => LANCE_INTERNAL_DST_FIELD,
        other => other,
    }
}

fn lance_dataset_kind(path: &Path) -> LanceDatasetKind {
    match path
        .parent()
        .and_then(|parent| parent.file_name())
        .and_then(|name| name.to_str())
    {
        Some("nodes") => LanceDatasetKind::Node,
        Some("edges") => LanceDatasetKind::Edge,
        _ => LanceDatasetKind::Plain,
    }
}

fn rename_batch_fields(batch: &RecordBatch, renames: &[(usize, &str)]) -> Result<RecordBatch> {
    let mut fields: Vec<arrow_schema::Field> = batch
        .schema()
        .fields()
        .iter()
        .map(|field| field.as_ref().clone())
        .collect();
    for (index, new_name) in renames {
        if *index >= fields.len() {
            return Err(NanoError::Storage(format!(
                "cannot rename field {} in schema with {} field(s)",
                index,
                fields.len()
            )));
        }
        fields[*index] = fields[*index].clone().with_name(*new_name);
    }

    RecordBatch::try_new(
        Arc::new(arrow_schema::Schema::new(fields)),
        batch.columns().to_vec(),
    )
    .map_err(|e| NanoError::Storage(format!("rename batch schema error: {}", e)))
}

fn logical_batch_to_lance(batch: &RecordBatch, kind: LanceDatasetKind) -> Result<RecordBatch> {
    match kind {
        LanceDatasetKind::Node => rename_batch_fields(batch, &[(0, LANCE_INTERNAL_ID_FIELD)]),
        LanceDatasetKind::Edge => rename_batch_fields(
            batch,
            &[
                (0, LANCE_INTERNAL_ID_FIELD),
                (1, LANCE_INTERNAL_SRC_FIELD),
                (2, LANCE_INTERNAL_DST_FIELD),
            ],
        ),
        LanceDatasetKind::Plain => Ok(batch.clone()),
    }
}

fn lance_batch_to_logical(batch: &RecordBatch, kind: LanceDatasetKind) -> Result<RecordBatch> {
    match kind {
        LanceDatasetKind::Node => {
            if batch.schema().field(0).name() == LANCE_INTERNAL_ID_FIELD {
                rename_batch_fields(batch, &[(0, "id")])
            } else {
                Ok(batch.clone())
            }
        }
        LanceDatasetKind::Edge => {
            if batch.schema().fields().len() >= 3
                && batch.schema().field(0).name() == LANCE_INTERNAL_ID_FIELD
                && batch.schema().field(1).name() == LANCE_INTERNAL_SRC_FIELD
                && batch.schema().field(2).name() == LANCE_INTERNAL_DST_FIELD
            {
                rename_batch_fields(batch, &[(0, "id"), (1, "src"), (2, "dst")])
            } else {
                Ok(batch.clone())
            }
        }
        LanceDatasetKind::Plain => Ok(batch.clone()),
    }
}

pub(crate) async fn write_lance_batch(path: &Path, batch: RecordBatch) -> Result<u64> {
    Ok(
        write_lance_batch_with_mode_versioned(path, batch, WriteMode::Overwrite)
            .await?
            .version,
    )
}

#[allow(dead_code)]
pub(crate) async fn write_lance_batch_with_mode(
    path: &Path,
    batch: RecordBatch,
    mode: WriteMode,
) -> Result<u64> {
    Ok(write_lance_batch_with_mode_versioned(path, batch, mode)
        .await?
        .version)
}

async fn write_lance_batch_with_mode_versioned(
    path: &Path,
    batch: RecordBatch,
    mode: WriteMode,
) -> Result<GraphTableVersion> {
    let storage_version = if path.exists() || matches!(mode, WriteMode::Append) {
        None
    } else {
        Some(DEFAULT_NEW_DATASET_STORAGE_VERSION)
    };
    write_lance_batch_with_mode_and_storage_version_versioned(path, batch, mode, storage_version)
        .await
}

#[allow(dead_code)]
pub(crate) async fn write_lance_batch_with_mode_and_storage_version(
    path: &Path,
    batch: RecordBatch,
    mode: WriteMode,
    storage_version: Option<LanceFileVersion>,
) -> Result<u64> {
    Ok(write_lance_batch_with_mode_and_storage_version_versioned(
        path,
        batch,
        mode,
        storage_version,
    )
    .await?
    .version)
}

async fn write_lance_batch_with_mode_and_storage_version_versioned(
    path: &Path,
    batch: RecordBatch,
    mode: WriteMode,
    storage_version: Option<LanceFileVersion>,
) -> Result<GraphTableVersion> {
    info!(
        dataset_path = %path.display(),
        rows = batch.num_rows(),
        mode = ?mode,
        "writing Lance dataset"
    );
    let kind = lance_dataset_kind(path);
    let batch = logical_batch_to_lance(&batch, kind)?;
    let schema = batch.schema();
    let uri = path.to_string_lossy().to_string();

    let reader = RecordBatchIterator::new(vec![Ok(batch)], schema);
    let write_params = match storage_version {
        Some(version) => {
            let mut params = WriteParams::with_storage_version(version);
            params.mode = mode;
            params
        }
        None => WriteParams {
            mode,
            ..Default::default()
        },
    };

    let dataset = Dataset::write(reader, &uri, Some(write_params))
        .await
        .map_err(|e| NanoError::Lance(format!("write error: {}", e)))?;

    Ok(GraphTableVersion::new(
        graph_table_id_for_path(path),
        dataset.version().version,
    ))
}

#[allow(dead_code)]
pub(crate) async fn run_lance_merge_insert_with_key(
    dataset_path: &Path,
    pinned_version: u64,
    source_batch: RecordBatch,
    key_prop: &str,
) -> Result<u64> {
    Ok(run_lance_merge_insert_with_key_versioned(
        dataset_path,
        &GraphTableVersion::new(graph_table_id_for_path(dataset_path), pinned_version),
        source_batch,
        key_prop,
    )
    .await?
    .version)
}

async fn run_lance_merge_insert_with_key_versioned(
    dataset_path: &Path,
    pinned_version: &GraphTableVersion,
    source_batch: RecordBatch,
    key_prop: &str,
) -> Result<GraphTableVersion> {
    let kind = lance_dataset_kind(dataset_path);
    let uri = dataset_path.to_string_lossy().to_string();
    let dataset = Dataset::open(&uri)
        .await
        .map_err(|e| NanoError::Lance(format!("merge open error: {}", e)))?;
    let dataset = dataset
        .checkout_version(pinned_version.version)
        .await
        .map_err(|e| {
            NanoError::Lance(format!(
                "merge checkout version {} error: {}",
                pinned_version.version, e
            ))
        })?;

    let mut builder = MergeInsertBuilder::try_new(Arc::new(dataset), vec![key_prop.to_string()])
        .map_err(|e| NanoError::Lance(format!("merge builder error: {}", e)))?;
    builder
        .when_matched(WhenMatched::UpdateAll)
        .when_not_matched(WhenNotMatched::InsertAll)
        .conflict_retries(0);

    let source_batch = logical_batch_to_lance(&source_batch, kind)?;
    let source_schema = source_batch.schema();
    let source = Box::new(RecordBatchIterator::new(
        vec![Ok(source_batch)].into_iter(),
        source_schema,
    ));
    let job = builder
        .try_build()
        .map_err(|e| NanoError::Lance(format!("merge build error: {}", e)))?;
    let (merged_dataset, _) = job
        .execute_reader(source)
        .await
        .map_err(|e| NanoError::Lance(format!("merge execute error: {}", e)))?;

    Ok(GraphTableVersion::new(
        graph_table_id_for_path(dataset_path),
        merged_dataset.version().version,
    ))
}

#[allow(dead_code)]
pub(crate) async fn run_lance_delete_by_ids(
    dataset_path: &Path,
    pinned_version: u64,
    ids: &[u64],
) -> Result<u64> {
    Ok(run_lance_delete_by_ids_versioned(
        dataset_path,
        &GraphTableVersion::new(graph_table_id_for_path(dataset_path), pinned_version),
        ids,
    )
    .await?
    .version)
}

async fn run_lance_delete_by_ids_versioned(
    dataset_path: &Path,
    pinned_version: &GraphTableVersion,
    ids: &[u64],
) -> Result<GraphTableVersion> {
    if ids.is_empty() {
        return Ok(pinned_version.clone());
    }

    let uri = dataset_path.to_string_lossy().to_string();
    let dataset = Dataset::open(&uri)
        .await
        .map_err(|e| NanoError::Lance(format!("delete open error: {}", e)))?;
    let mut dataset = dataset
        .checkout_version(pinned_version.version)
        .await
        .map_err(|e| {
            NanoError::Lance(format!(
                "delete checkout version {} error: {}",
                pinned_version.version, e
            ))
        })?;

    let predicate = ids
        .iter()
        .map(|id| format!("{} = {}", LANCE_INTERNAL_ID_FIELD, id))
        .collect::<Vec<_>>()
        .join(" OR ");
    dataset
        .delete(&predicate)
        .await
        .map_err(|e| NanoError::Lance(format!("delete execute error: {}", e)))?;

    Ok(GraphTableVersion::new(
        graph_table_id_for_path(dataset_path),
        dataset.version().version,
    ))
}

pub(crate) async fn latest_lance_dataset_version(path: &Path) -> Result<u64> {
    Ok(latest_lance_dataset_graph_version(path).await?.version)
}

async fn latest_lance_dataset_graph_version(path: &Path) -> Result<GraphTableVersion> {
    let uri = path.to_string_lossy().to_string();
    let dataset = Dataset::open(&uri)
        .await
        .map_err(|e| NanoError::Lance(format!("open error: {}", e)))?;
    let version = dataset
        .latest_version_id()
        .await
        .map_err(|e| NanoError::Lance(format!("latest version error: {}", e)))?;
    Ok(GraphTableVersion::new(
        graph_table_id_for_path(path),
        version,
    ))
}

pub(crate) async fn read_lance_batches(path: &Path, version: u64) -> Result<Vec<RecordBatch>> {
    read_lance_batches_versioned(&GraphTableVersion::new(
        graph_table_id_for_path(path),
        version,
    ))
    .await
}

async fn read_lance_batches_versioned(version: &GraphTableVersion) -> Result<Vec<RecordBatch>> {
    info!(
        dataset_path = %version.table_id.as_str(),
        dataset_version = version.version,
        "reading Lance dataset"
    );
    let path = Path::new(version.table_id.as_str());
    let uri = path.to_string_lossy().to_string();
    let dataset = Dataset::open(&uri)
        .await
        .map_err(|e| NanoError::Lance(format!("open error: {}", e)))?;
    let dataset = dataset
        .checkout_version(version.version)
        .await
        .map_err(|e| {
            NanoError::Lance(format!("checkout version {} error: {}", version.version, e))
        })?;

    let kind = lance_dataset_kind(path);
    let scanner = dataset.scan();
    let batches: Vec<RecordBatch> = scanner
        .try_into_stream()
        .await
        .map_err(|e| NanoError::Lance(format!("scan error: {}", e)))?
        .map(|batch| batch.map_err(|e| NanoError::Lance(format!("stream error: {}", e))))
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect::<Result<Vec<_>>>()?
        .into_iter()
        .map(|batch| lance_batch_to_logical(&batch, kind))
        .collect::<Result<Vec<_>>>()?;

    Ok(batches)
}

pub(crate) async fn read_lance_projected_batches(
    path: &Path,
    version: u64,
    columns: &[&str],
) -> Result<Vec<RecordBatch>> {
    info!(
        dataset_path = %path.display(),
        dataset_version = version,
        projection = ?columns,
        "reading projected Lance dataset"
    );
    let uri = path.to_string_lossy().to_string();
    let dataset = Dataset::open(&uri)
        .await
        .map_err(|e| NanoError::Lance(format!("open error: {}", e)))?;
    let dataset = dataset
        .checkout_version(version)
        .await
        .map_err(|e| NanoError::Lance(format!("checkout version {} error: {}", version, e)))?;

    let kind = lance_dataset_kind(path);
    let projected_columns: Vec<String> = columns
        .iter()
        .map(|column| match kind {
            LanceDatasetKind::Node => logical_node_field_to_lance(column),
            LanceDatasetKind::Edge => logical_edge_field_to_lance(column),
            LanceDatasetKind::Plain => column,
        })
        .map(ToString::to_string)
        .collect();

    let mut scanner = dataset.scan();
    scanner
        .project(&projected_columns)
        .map_err(|e| NanoError::Lance(format!("projection error: {}", e)))?;
    let batches: Vec<RecordBatch> = scanner
        .try_into_stream()
        .await
        .map_err(|e| NanoError::Lance(format!("scan error: {}", e)))?
        .map(|batch| batch.map_err(|e| NanoError::Lance(format!("stream error: {}", e))))
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect::<Result<Vec<_>>>()?
        .into_iter()
        .map(|batch| lance_batch_to_logical(&batch, kind))
        .collect::<Result<Vec<_>>>()?;

    Ok(batches)
}

#[async_trait]
impl TableStore for V3TableStore {
    async fn overwrite(&self, path: &Path, batch: RecordBatch) -> Result<GraphTableVersion> {
        write_lance_batch_with_mode_versioned(path, batch, WriteMode::Overwrite).await
    }

    async fn append(&self, path: &Path, batch: RecordBatch) -> Result<GraphTableVersion> {
        write_lance_batch_with_mode_versioned(path, batch, WriteMode::Append).await
    }

    async fn merge_insert_with_key(
        &self,
        path: &Path,
        pinned_version: &GraphTableVersion,
        source_batch: RecordBatch,
        key_prop: &str,
    ) -> Result<GraphTableVersion> {
        run_lance_merge_insert_with_key_versioned(path, pinned_version, source_batch, key_prop)
            .await
    }

    async fn delete_by_ids(
        &self,
        path: &Path,
        pinned_version: &GraphTableVersion,
        ids: &[u64],
    ) -> Result<GraphTableVersion> {
        run_lance_delete_by_ids_versioned(path, pinned_version, ids).await
    }

    async fn latest_version(&self, path: &Path) -> Result<GraphTableVersion> {
        latest_lance_dataset_graph_version(path).await
    }

    async fn read_all_batches(&self, version: &GraphTableVersion) -> Result<Vec<RecordBatch>> {
        read_lance_batches_versioned(version).await
    }
}

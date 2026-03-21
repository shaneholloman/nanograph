use std::ops::Deref;
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};

use ahash::AHashMap;
use arrow_array::{BooleanArray, RecordBatch};
use serde::{Deserialize, Serialize};
use tempfile::TempDir;
use tokio::sync::Mutex;
use tracing::{debug, info, instrument};

use crate::catalog::Catalog;
use crate::catalog::schema_ir::{SchemaIR, build_catalog_from_ir, build_schema_ir};
use crate::error::{NanoError, Result};
use crate::ir::{ParamMap, QueryIR};
use crate::plan::physical::execute_mutation;
use crate::plan::planner::execute_query_with_edge_index_cache;
use crate::query::ast::{Literal, NOW_PARAM_NAME, QueryDecl};
use crate::query::parser::parse_query;
use crate::query::typecheck::{
    CheckedQuery, infer_query_result_schema, typecheck_query, typecheck_query_decl,
};
use crate::query_input::{
    JsonParamMode, RunInputResult, find_named_query, json_params_to_param_map,
};
use crate::result::{MutationResult, QueryResult, RunResult};
use crate::schema::parser::parse_schema;
use crate::store::csr::CsrIndex;
use crate::store::graph::GraphStorage;
use crate::store::lance_io::{read_lance_batches, read_lance_projected_batches};
use crate::store::manifest::{GraphManifest, hash_string};
use crate::store::metadata::{DatabaseMetadata, SCHEMA_IR_FILENAME};
use crate::{lower_mutation_query, lower_query};

const SCHEMA_PG_FILENAME: &str = "schema.pg";
const CDC_ANALYTICS_DATASET_DIR: &str = "__cdc_analytics";
const CDC_ANALYTICS_STATE_FILE: &str = "__cdc_analytics.state.json";

pub(crate) mod cdc;
mod maintenance;
pub(crate) mod mutation;
mod persist;

pub use maintenance::{cleanup_database, compact_database};
pub(crate) use mutation::DatasetMutationPlan;
pub use persist::{load_database_file_sparse, run_mutation_query_sparse};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeleteOp {
    Eq,
    Ne,
    Gt,
    Ge,
    Lt,
    Le,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LoadMode {
    Overwrite,
    Append,
    Merge,
}

#[derive(Debug, Clone)]
pub struct DeletePredicate {
    pub property: String,
    pub op: DeleteOp,
    pub value: String,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct DeleteResult {
    pub deleted_nodes: usize,
    pub deleted_edges: usize,
}

#[derive(Debug, Clone, Copy)]
pub struct CompactOptions {
    pub target_rows_per_fragment: usize,
    pub materialize_deletions: bool,
    pub materialize_deletions_threshold: f32,
}

impl Default for CompactOptions {
    fn default() -> Self {
        Self {
            target_rows_per_fragment: 1_048_576,
            materialize_deletions: true,
            materialize_deletions_threshold: 0.1,
        }
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct CompactResult {
    pub datasets_considered: usize,
    pub datasets_compacted: usize,
    pub fragments_removed: usize,
    pub fragments_added: usize,
    pub files_removed: usize,
    pub files_added: usize,
    pub manifest_committed: bool,
}

#[derive(Debug, Clone, Copy)]
pub struct CleanupOptions {
    pub retain_tx_versions: u64,
    pub retain_dataset_versions: usize,
}

impl Default for CleanupOptions {
    fn default() -> Self {
        Self {
            retain_tx_versions: 128,
            retain_dataset_versions: 2,
        }
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct CleanupResult {
    pub tx_rows_removed: usize,
    pub tx_rows_kept: usize,
    pub cdc_rows_removed: usize,
    pub cdc_rows_kept: usize,
    pub datasets_cleaned: usize,
    pub dataset_old_versions_removed: u64,
    pub dataset_bytes_removed: u64,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct CdcAnalyticsMaterializeOptions {
    pub min_new_rows: usize,
    pub force: bool,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct CdcAnalyticsMaterializeResult {
    pub source_rows: usize,
    pub previously_materialized_rows: usize,
    pub new_rows_since_last_run: usize,
    pub materialized_rows: usize,
    pub dataset_written: bool,
    pub skipped_by_threshold: bool,
    pub dataset_version: Option<u64>,
}

#[derive(Debug, Clone, Default)]
pub struct EmbedOptions {
    pub type_name: Option<String>,
    pub property: Option<String>,
    pub only_null: bool,
    pub limit: Option<usize>,
    pub reindex: bool,
    pub dry_run: bool,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct EmbedResult {
    pub node_types_considered: usize,
    pub properties_selected: usize,
    pub rows_selected: usize,
    pub embeddings_generated: usize,
    pub reindexed_types: usize,
    pub dry_run: bool,
}

#[derive(Debug, Clone, Default)]
pub struct DoctorDatasetReport {
    pub kind: String,
    pub type_name: String,
    pub dataset_path: String,
    pub dataset_version: u64,
    pub storage_version: String,
}

#[derive(Debug, Clone, Default)]
pub struct DoctorReport {
    pub healthy: bool,
    pub issues: Vec<String>,
    pub warnings: Vec<String>,
    pub manifest_db_version: u64,
    pub datasets_checked: usize,
    pub datasets: Vec<DoctorDatasetReport>,
    pub tx_rows: usize,
    pub cdc_rows: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct CdcAnalyticsState {
    rows_materialized: usize,
    manifest_db_version: u64,
    dataset_version: Option<u64>,
    updated_at_unix: String,
}

#[derive(Debug, Clone)]
enum MutationSource {
    LoadString { mode: LoadMode, data_source: String },
    LoadFile { mode: LoadMode, data_path: PathBuf },
    PreparedDatasets(DatasetMutationPlan),
}

#[derive(Debug, Clone)]
struct MutationPlan {
    source: MutationSource,
    op_summary: String,
}

impl MutationPlan {
    fn for_load(data_source: &str, mode: LoadMode) -> Self {
        Self {
            source: MutationSource::LoadString {
                mode,
                data_source: data_source.to_string(),
            },
            op_summary: load_mode_op_summary(mode).to_string(),
        }
    }

    fn for_load_file(data_path: &Path, mode: LoadMode) -> Self {
        Self {
            source: MutationSource::LoadFile {
                mode,
                data_path: data_path.to_path_buf(),
            },
            op_summary: load_mode_op_summary(mode).to_string(),
        }
    }

    fn append_mutation(data_source: &str, op_summary: &str) -> Self {
        Self {
            source: MutationSource::LoadString {
                mode: LoadMode::Append,
                data_source: data_source.to_string(),
            },
            op_summary: op_summary.to_string(),
        }
    }

    fn merge_mutation(data_source: &str, op_summary: &str) -> Self {
        Self {
            source: MutationSource::LoadString {
                mode: LoadMode::Merge,
                data_source: data_source.to_string(),
            },
            op_summary: op_summary.to_string(),
        }
    }

    fn prepared_datasets(plan: DatasetMutationPlan) -> Self {
        Self {
            op_summary: plan.op_summary.clone(),
            source: MutationSource::PreparedDatasets(plan),
        }
    }
}

pub struct DatabaseShared {
    path: PathBuf,
    tempdir: Option<TempDir>,
    pub schema_ir: SchemaIR,
    pub catalog: Catalog,
    storage: RwLock<Arc<GraphStorage>>,
    writer: Mutex<()>,
    edge_index_cache: Arc<EdgeIndexCache>,
}

#[derive(Clone)]
pub struct Database {
    inner: Arc<DatabaseShared>,
}

pub(crate) struct DatabaseWriteGuard<'a> {
    _guard: tokio::sync::MutexGuard<'a, ()>,
}

#[derive(Debug)]
pub(crate) struct EdgeIndexPair {
    pub(crate) csr: Arc<CsrIndex>,
    pub(crate) csc: Arc<CsrIndex>,
}

#[derive(Debug, Default)]
pub(crate) struct EdgeIndexCache {
    inner: Mutex<AHashMap<(String, u64), Arc<EdgeIndexPair>>>,
}

impl EdgeIndexCache {
    pub(crate) async fn get_or_build(
        &self,
        edge_type: &str,
        dataset_path: &Path,
        dataset_version: u64,
        max_node_id: u64,
    ) -> Result<Arc<EdgeIndexPair>> {
        let key = (edge_type.to_string(), dataset_version);
        let mut guard = self.inner.lock().await;
        if let Some(pair) = guard.get(&key) {
            return Ok(pair.clone());
        }

        let batches =
            read_lance_projected_batches(dataset_path, dataset_version, &["id", "src", "dst"])
                .await?;
        let mut out_edges = Vec::new();
        let mut in_edges = Vec::new();
        for batch in batches {
            let id_arr = batch
                .column_by_name("id")
                .ok_or_else(|| NanoError::Storage("edge batch missing id column".to_string()))?
                .as_any()
                .downcast_ref::<arrow_array::UInt64Array>()
                .ok_or_else(|| NanoError::Storage("edge id column is not UInt64".to_string()))?;
            let src_arr = batch
                .column_by_name("src")
                .ok_or_else(|| NanoError::Storage("edge batch missing src column".to_string()))?
                .as_any()
                .downcast_ref::<arrow_array::UInt64Array>()
                .ok_or_else(|| NanoError::Storage("edge src column is not UInt64".to_string()))?;
            let dst_arr = batch
                .column_by_name("dst")
                .ok_or_else(|| NanoError::Storage("edge batch missing dst column".to_string()))?
                .as_any()
                .downcast_ref::<arrow_array::UInt64Array>()
                .ok_or_else(|| NanoError::Storage("edge dst column is not UInt64".to_string()))?;

            for row in 0..batch.num_rows() {
                let edge_id = id_arr.value(row);
                let src = src_arr.value(row);
                let dst = dst_arr.value(row);
                out_edges.push((src, dst, edge_id));
                in_edges.push((dst, src, edge_id));
            }
        }

        let pair = Arc::new(EdgeIndexPair {
            csr: Arc::new(CsrIndex::build(max_node_id as usize, &mut out_edges)),
            csc: Arc::new(CsrIndex::build(max_node_id as usize, &mut in_edges)),
        });
        guard.insert(key, pair.clone());
        Ok(pair)
    }
}

#[derive(Debug, Clone)]
pub struct PreparedReadQuery {
    ir: QueryIR,
    output_schema: arrow_schema::SchemaRef,
    storage: Arc<GraphStorage>,
    edge_index_cache: Arc<EdgeIndexCache>,
}

impl PreparedReadQuery {
    fn new(
        ir: QueryIR,
        output_schema: arrow_schema::SchemaRef,
        storage: Arc<GraphStorage>,
        edge_index_cache: Arc<EdgeIndexCache>,
    ) -> Self {
        Self {
            ir,
            output_schema,
            storage,
            edge_index_cache,
        }
    }

    pub async fn execute(&self, params: &ParamMap) -> Result<QueryResult> {
        let runtime_params = params_with_runtime_now(params)?;
        let batches = execute_query_with_edge_index_cache(
            &self.ir,
            self.storage.clone(),
            &runtime_params,
            self.edge_index_cache.clone(),
        )
        .await?;
        Ok(QueryResult::new(self.output_schema.clone(), batches))
    }

    pub fn ir(&self) -> &QueryIR {
        &self.ir
    }

    pub fn output_schema(&self) -> &arrow_schema::SchemaRef {
        &self.output_schema
    }
}

impl Deref for Database {
    type Target = DatabaseShared;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl Database {
    /// Create a new database directory from schema source text.
    #[instrument(skip(schema_source), fields(db_path = %db_path.display()))]
    pub async fn init(db_path: &Path, schema_source: &str) -> Result<Self> {
        Self::init_internal(db_path, schema_source, None).await
    }

    /// Create a new tempdir-backed database that cleans itself up when dropped.
    pub async fn open_in_memory(schema_source: &str) -> Result<Self> {
        let tempdir = tempfile::Builder::new()
            .prefix("nanograph_in_memory_")
            .tempdir()?;
        let temp_path = tempdir.path().to_path_buf();
        Self::init_internal(&temp_path, schema_source, Some(tempdir)).await
    }

    async fn init_internal(
        db_path: &Path,
        schema_source: &str,
        tempdir: Option<TempDir>,
    ) -> Result<Self> {
        info!("initializing database");
        // Parse and validate schema
        let schema_file = parse_schema(schema_source)?;
        let schema_ir = build_schema_ir(&schema_file)?;
        let catalog = build_catalog_from_ir(&schema_ir)?;

        // Create directory structure
        std::fs::create_dir_all(db_path)?;
        std::fs::create_dir_all(db_path.join("nodes"))?;
        std::fs::create_dir_all(db_path.join("edges"))?;

        // Write schema.pg (human-authored source)
        std::fs::write(db_path.join(SCHEMA_PG_FILENAME), schema_source)?;

        // Write schema.ir.json
        let ir_json = serde_json::to_string_pretty(&schema_ir)
            .map_err(|e| NanoError::Manifest(format!("serialize IR error: {}", e)))?;
        std::fs::write(db_path.join(SCHEMA_IR_FILENAME), &ir_json)?;

        // Write empty manifest
        let ir_hash = hash_string(&ir_json);
        let mut manifest = GraphManifest::new(ir_hash);
        let (next_type_id, next_prop_id) = next_schema_identity_counters(&schema_ir);
        manifest.next_type_id = next_type_id;
        manifest.next_prop_id = next_prop_id;
        manifest.committed_at = now_unix_seconds_string();
        manifest.write_atomic(db_path)?;

        let storage = GraphStorage::new(catalog.clone());
        info!("database initialized");

        Ok(Self::from_parts(
            db_path.to_path_buf(),
            schema_ir,
            catalog,
            storage,
            tempdir,
        ))
    }

    /// Open an existing database.
    #[instrument(fields(db_path = %db_path.display()))]
    pub async fn open(db_path: &Path) -> Result<Self> {
        info!("opening database");
        let metadata = DatabaseMetadata::open(db_path)?;
        let schema_ir = metadata.schema_ir().clone();
        let catalog = metadata.catalog().clone();
        let manifest = metadata.manifest().clone();

        // Create storage and set ID counters
        let mut storage = GraphStorage::new(catalog.clone());
        storage.set_next_node_id(manifest.next_node_id);
        storage.set_next_edge_id(manifest.next_edge_id);

        // Load only datasets listed in the manifest (authoritative source)
        for entry in &manifest.datasets {
            let dataset_path = db_path.join(&entry.dataset_path);
            debug!(
                kind = %entry.kind,
                type_name = %entry.type_name,
                dataset_path = %dataset_path.display(),
                dataset_version = entry.dataset_version,
                row_count = entry.row_count,
                "restoring dataset from manifest"
            );
            match entry.kind.as_str() {
                "node" => {
                    let batches = read_lance_batches(&dataset_path, entry.dataset_version).await?;
                    for batch in batches {
                        storage.load_node_batch(&entry.type_name, batch)?;
                    }
                    storage.set_node_dataset_path(&entry.type_name, dataset_path);
                    storage.set_node_dataset_version(&entry.type_name, entry.dataset_version);
                }
                "edge" => {
                    let batches = read_lance_batches(&dataset_path, entry.dataset_version).await?;
                    for batch in batches {
                        storage.load_edge_batch(&entry.type_name, batch)?;
                    }
                    storage.set_edge_dataset_path(&entry.type_name, dataset_path);
                    storage.set_edge_dataset_version(&entry.type_name, entry.dataset_version);
                }
                other => {
                    return Err(NanoError::Manifest(format!(
                        "unknown dataset kind `{}` for type `{}`",
                        other, entry.type_name
                    )));
                }
            }
        }

        info!(
            node_types = storage.node_segments.len(),
            edge_types = storage.edge_segments.len(),
            "database open complete"
        );

        Ok(Self::from_parts(
            db_path.to_path_buf(),
            schema_ir,
            catalog,
            storage,
            None,
        ))
    }

    fn from_parts(
        path: PathBuf,
        schema_ir: SchemaIR,
        catalog: Catalog,
        storage: GraphStorage,
        tempdir: Option<TempDir>,
    ) -> Self {
        Database {
            inner: Arc::new(DatabaseShared {
                path,
                tempdir,
                schema_ir,
                catalog,
                storage: RwLock::new(Arc::new(storage)),
                writer: Mutex::new(()),
                edge_index_cache: Arc::new(EdgeIndexCache::default()),
            }),
        }
    }

    /// Get catalog reference for typechecking.
    pub fn catalog(&self) -> &Catalog {
        &self.catalog
    }

    pub fn schema_ir(&self) -> &SchemaIR {
        &self.schema_ir
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn is_in_memory(&self) -> bool {
        self.tempdir.is_some()
    }

    pub(crate) async fn lock_writer(&self) -> DatabaseWriteGuard<'_> {
        DatabaseWriteGuard {
            _guard: self.writer.lock().await,
        }
    }

    fn snapshot_storage(&self) -> Arc<GraphStorage> {
        self.storage
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
    }

    fn replace_storage(&self, storage: GraphStorage) {
        *self
            .storage
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Arc::new(storage);
    }

    /// Clone the current storage snapshot for query execution.
    pub fn snapshot(&self) -> Arc<GraphStorage> {
        self.snapshot_storage()
    }

    fn prepare_read_query_with_storage(
        &self,
        query: &QueryDecl,
        storage: Arc<GraphStorage>,
    ) -> Result<PreparedReadQuery> {
        let catalog = self.catalog().clone();
        let type_ctx = typecheck_query(&catalog, query)?;
        let output_schema = infer_query_result_schema(&catalog, query, &type_ctx)?;
        let ir = lower_query(&catalog, query, &type_ctx)?;
        Ok(PreparedReadQuery::new(
            ir,
            output_schema,
            storage,
            self.edge_index_cache.clone(),
        ))
    }

    pub fn prepare_read_query(&self, query: &QueryDecl) -> Result<PreparedReadQuery> {
        let mut prepared_storage = self.snapshot().as_ref().clone();
        prepared_storage.clear_node_dataset_paths();
        self.prepare_read_query_with_storage(query, Arc::new(prepared_storage))
    }

    pub async fn run_query(&self, query: &QueryDecl, params: &ParamMap) -> Result<RunResult> {
        if query.mutation.is_some() {
            let checked = typecheck_query_decl(self.catalog(), query)?;
            if !matches!(checked, CheckedQuery::Mutation(_)) {
                return Err(NanoError::Type("expected mutation query".to_string()));
            }

            let mutation_ir = lower_mutation_query(query)?;
            let mut writer = self.lock_writer().await;
            let runtime_params = params_with_runtime_now(params)?;
            let result = execute_mutation(&mutation_ir, self, &runtime_params, &mut writer).await?;
            return Ok(RunResult::Mutation(MutationResult::from(result)));
        }

        let prepared = self.prepare_read_query_with_storage(query, self.snapshot())?;
        let result = prepared.execute(params).await?;
        Ok(RunResult::Query(result))
    }

    pub async fn run(
        &self,
        query_source: &str,
        query_name: &str,
        params: &ParamMap,
    ) -> Result<RunResult> {
        let queries = parse_query(query_source)?;
        let query = queries
            .queries
            .into_iter()
            .find(|query| query.name == query_name)
            .ok_or_else(|| NanoError::Execution(format!("query '{}' not found", query_name)))?;
        self.run_query(&query, params).await
    }

    pub async fn run_json(
        &self,
        query_source: &str,
        query_name: &str,
        params: Option<&serde_json::Value>,
        mode: JsonParamMode,
    ) -> RunInputResult<RunResult> {
        let query = find_named_query(query_source, query_name)?;
        let params = json_params_to_param_map(params, &query.params, mode)?;
        self.run_query(&query, &params).await.map_err(Into::into)
    }
}

pub(crate) fn build_delete_mask_for_mutation(
    batch: &RecordBatch,
    predicate: &DeletePredicate,
) -> Result<BooleanArray> {
    cdc::build_delete_mask_for_mutation(batch, predicate)
}

#[cfg(test)]
fn trim_surrounding_quotes(s: &str) -> &str {
    cdc::trim_surrounding_quotes(s)
}

fn now_unix_seconds_string() -> String {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs().to_string())
        .unwrap_or_else(|_| "0".to_string())
}

fn runtime_now_literal() -> Result<Literal> {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|e| NanoError::Execution(format!("failed to read system time: {}", e)))?;
    let millis = i64::try_from(now.as_millis())
        .map_err(|_| NanoError::Execution("system time exceeds supported range".to_string()))?;
    let dt = arrow_array::temporal_conversions::date64_to_datetime(millis).ok_or_else(|| {
        NanoError::Execution("failed to convert system time to DateTime literal".to_string())
    })?;
    Ok(Literal::DateTime(
        dt.format("%Y-%m-%dT%H:%M:%S%.3fZ").to_string(),
    ))
}

fn params_with_runtime_now(params: &ParamMap) -> Result<ParamMap> {
    let mut runtime_params = params.clone();
    runtime_params.insert(NOW_PARAM_NAME.to_string(), runtime_now_literal()?);
    Ok(runtime_params)
}

fn load_mode_op_summary(mode: LoadMode) -> &'static str {
    match mode {
        LoadMode::Overwrite => "load:overwrite",
        LoadMode::Append => "load:append",
        LoadMode::Merge => "load:merge",
    }
}

fn next_schema_identity_counters(ir: &SchemaIR) -> (u32, u32) {
    use crate::catalog::schema_ir::TypeDef;

    let mut max_type_id = 0u32;
    let mut max_prop_id = 0u32;
    for ty in &ir.types {
        match ty {
            TypeDef::Node(n) => {
                max_type_id = max_type_id.max(n.type_id);
                for p in &n.properties {
                    max_prop_id = max_prop_id.max(p.prop_id);
                }
            }
            TypeDef::Edge(e) => {
                max_type_id = max_type_id.max(e.type_id);
                for p in &e.properties {
                    max_prop_id = max_prop_id.max(p.prop_id);
                }
            }
        }
    }
    (
        max_type_id.saturating_add(1).max(1),
        max_prop_id.saturating_add(1).max(1),
    )
}

#[cfg(test)]
mod tests;

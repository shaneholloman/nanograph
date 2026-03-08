use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};

use arrow_array::{ArrayRef, RecordBatch};
use color_eyre::eyre::{Result, WrapErr, eyre};
use tracing::instrument;

use nanograph::store::database::Database;
use nanograph::store::manifest::GraphManifest;

#[instrument(fields(db = ?db_path.as_ref().map(|p| p.display().to_string())))]
pub(crate) async fn cmd_version(db_path: Option<PathBuf>, json: bool) -> Result<()> {
    let payload = build_version_payload(db_path.as_deref())?;

    if json {
        let out =
            serde_json::to_string_pretty(&payload).wrap_err("failed to serialize version JSON")?;
        println!("{}", out);
        return Ok(());
    }

    print_version_table(&payload);
    Ok(())
}

pub(crate) fn build_version_payload(db_path: Option<&Path>) -> Result<serde_json::Value> {
    let mut payload = serde_json::json!({
        "binary_version": env!("CARGO_PKG_VERSION"),
    });

    if let Some(path) = db_path {
        let manifest = GraphManifest::read(path)?;
        let dataset_versions = manifest
            .datasets
            .iter()
            .map(|entry| {
                serde_json::json!({
                    "kind": entry.kind,
                    "type_name": entry.type_name,
                    "type_id": entry.type_id,
                    "dataset_path": entry.dataset_path,
                    "dataset_version": entry.dataset_version,
                    "row_count": entry.row_count,
                })
            })
            .collect::<Vec<_>>();
        payload["db"] = serde_json::json!({
            "path": path.display().to_string(),
            "format_version": manifest.format_version,
            "db_version": manifest.db_version,
            "last_tx_id": manifest.last_tx_id,
            "committed_at": manifest.committed_at,
            "schema_ir_hash": manifest.schema_ir_hash,
            "schema_identity_version": manifest.schema_identity_version,
            "next_node_id": manifest.next_node_id,
            "next_edge_id": manifest.next_edge_id,
            "next_type_id": manifest.next_type_id,
            "next_prop_id": manifest.next_prop_id,
            "dataset_count": manifest.datasets.len(),
            "dataset_versions": dataset_versions,
        });
    }

    Ok(payload)
}

fn print_version_table(payload: &serde_json::Value) {
    println!(
        "nanograph {}",
        payload["binary_version"].as_str().unwrap_or_default()
    );
    if let Some(db) = payload.get("db") {
        println!("Database: {}", db["path"].as_str().unwrap_or_default());
        println!(
            "Manifest: format v{}, db_version {}",
            db["format_version"].as_u64().unwrap_or(0),
            db["db_version"].as_u64().unwrap_or(0)
        );
        println!(
            "Last TX: {} @ {}",
            db["last_tx_id"].as_str().unwrap_or_default(),
            db["committed_at"].as_str().unwrap_or_default()
        );
        println!(
            "Schema hash: {} (identity v{})",
            db["schema_ir_hash"].as_str().unwrap_or_default(),
            db["schema_identity_version"].as_u64().unwrap_or(0)
        );
        println!(
            "Next IDs: node={} edge={} type={} prop={}",
            db["next_node_id"].as_u64().unwrap_or(0),
            db["next_edge_id"].as_u64().unwrap_or(0),
            db["next_type_id"].as_u64().unwrap_or(0),
            db["next_prop_id"].as_u64().unwrap_or(0)
        );
        println!("Datasets: {}", db["dataset_count"].as_u64().unwrap_or(0));
        if let Some(entries) = db["dataset_versions"].as_array() {
            for entry in entries {
                println!(
                    "  - {} {}: v{} (rows={})",
                    entry["kind"].as_str().unwrap_or_default(),
                    entry["type_name"].as_str().unwrap_or_default(),
                    entry["dataset_version"].as_u64().unwrap_or(0),
                    entry["row_count"].as_u64().unwrap_or(0),
                );
            }
        }
    }
}

#[instrument(fields(db_path = %db_path.display(), format = format))]
pub(crate) async fn cmd_describe(
    db_path: PathBuf,
    format: &str,
    json: bool,
    type_name: Option<&str>,
) -> Result<()> {
    let db = Database::open(&db_path).await?;
    let manifest = GraphManifest::read(&db_path)?;
    let payload = build_describe_payload(&db_path, &db, &manifest, type_name)?;
    let effective_format = if json { "json" } else { format };

    match effective_format {
        "json" => {
            let out = serde_json::to_string_pretty(&payload)
                .wrap_err("failed to serialize describe JSON")?;
            println!("{}", out);
        }
        "table" => print_describe_table(&payload),
        other => return Err(eyre!("unknown format: {} (supported: table, json)", other)),
    }

    Ok(())
}

pub(crate) fn build_describe_payload(
    db_path: &Path,
    db: &Database,
    manifest: &GraphManifest,
    type_name: Option<&str>,
) -> Result<serde_json::Value> {
    let storage = db.snapshot();
    let dataset_map = manifest
        .datasets
        .iter()
        .map(|d| ((d.kind.clone(), d.type_name.clone()), d))
        .collect::<HashMap<_, _>>();

    let mut nodes = Vec::new();
    for node in db.schema_ir.node_types() {
        if let Some(type_name) = type_name
            && node.name != type_name
        {
            continue;
        }
        let rows = storage
            .get_all_nodes(&node.name)?
            .map(|b| b.num_rows() as u64)
            .unwrap_or(0);
        let dataset = dataset_map.get(&("node".to_string(), node.name.clone()));
        let properties = node
            .properties
            .iter()
            .map(|prop| {
                serde_json::json!({
                    "name": prop.name,
                    "prop_id": prop.prop_id,
                    "type": prop_type_string(prop),
                    "key": prop.key,
                    "unique": prop.unique,
                    "index": prop.index,
                    "embed_source": prop.embed_source,
                    "description": prop.description,
                })
            })
            .collect::<Vec<_>>();
        let outgoing_edges = db
            .schema_ir
            .edge_types()
            .filter(|edge| edge.src_type_name == node.name)
            .map(|edge| {
                serde_json::json!({
                    "name": edge.name,
                    "to_type": edge.dst_type_name,
                })
            })
            .collect::<Vec<_>>();
        let incoming_edges = db
            .schema_ir
            .edge_types()
            .filter(|edge| edge.dst_type_name == node.name)
            .map(|edge| {
                serde_json::json!({
                    "name": edge.name,
                    "from_type": edge.src_type_name,
                })
            })
            .collect::<Vec<_>>();
        nodes.push(serde_json::json!({
            "name": node.name,
            "type_id": node.type_id,
            "description": node.description,
            "instruction": node.instruction,
            "key_property": node.key_property_name(),
            "unique_properties": node.unique_properties().map(|prop| prop.name.clone()).collect::<Vec<_>>(),
            "outgoing_edges": outgoing_edges,
            "incoming_edges": incoming_edges,
            "rows": rows,
            "dataset_path": dataset.map(|d| d.dataset_path.clone()),
            "dataset_version": dataset.map(|d| d.dataset_version),
            "properties": properties,
        }));
    }

    let mut edges = Vec::new();
    for edge in db.schema_ir.edge_types() {
        if let Some(type_name) = type_name
            && edge.name != type_name
        {
            continue;
        }
        let rows = storage
            .edge_batch_for_save(&edge.name)?
            .map(|b| b.num_rows() as u64)
            .unwrap_or(0);
        let dataset = dataset_map.get(&("edge".to_string(), edge.name.clone()));
        let properties = edge
            .properties
            .iter()
            .map(|prop| {
                serde_json::json!({
                    "name": prop.name,
                    "prop_id": prop.prop_id,
                    "type": prop_type_string(prop),
                    "description": prop.description,
                })
            })
            .collect::<Vec<_>>();
        edges.push(serde_json::json!({
            "name": edge.name,
            "type_id": edge.type_id,
            "src_type": edge.src_type_name,
            "dst_type": edge.dst_type_name,
            "description": edge.description,
            "instruction": edge.instruction,
            "endpoint_keys": {
                "src": db.schema_ir.node_key_property_name(&edge.src_type_name),
                "dst": db.schema_ir.node_key_property_name(&edge.dst_type_name),
            },
            "rows": rows,
            "dataset_path": dataset.map(|d| d.dataset_path.clone()),
            "dataset_version": dataset.map(|d| d.dataset_version),
            "properties": properties,
        }));
    }

    if let Some(type_name) = type_name
        && nodes.is_empty()
        && edges.is_empty()
    {
        return Err(eyre!("type `{}` not found in schema", type_name));
    }

    Ok(serde_json::json!({
        "db_path": db_path.display().to_string(),
        "binary_version": env!("CARGO_PKG_VERSION"),
        "type_filter": type_name,
        "manifest": {
            "format_version": manifest.format_version,
            "db_version": manifest.db_version,
            "last_tx_id": manifest.last_tx_id,
            "committed_at": manifest.committed_at,
            "schema_ir_hash": manifest.schema_ir_hash,
            "schema_identity_version": manifest.schema_identity_version,
            "datasets": manifest.datasets.len(),
        },
        "schema_ir_version": db.schema_ir.ir_version,
        "nodes": nodes,
        "edges": edges,
    }))
}

fn print_describe_table(payload: &serde_json::Value) {
    println!(
        "Database: {}",
        payload["db_path"].as_str().unwrap_or_default()
    );
    println!(
        "Manifest: format v{}, db_version {}",
        payload["manifest"]["format_version"].as_u64().unwrap_or(0),
        payload["manifest"]["db_version"].as_u64().unwrap_or(0)
    );
    println!(
        "Last TX: {} @ {}",
        payload["manifest"]["last_tx_id"]
            .as_str()
            .unwrap_or_default(),
        payload["manifest"]["committed_at"]
            .as_str()
            .unwrap_or_default()
    );
    println!(
        "Schema: ir_version {}, hash {}",
        payload["schema_ir_version"].as_u64().unwrap_or(0),
        payload["manifest"]["schema_ir_hash"]
            .as_str()
            .unwrap_or_default()
    );
    println!();

    println!("Node Types");
    if let Some(nodes) = payload["nodes"].as_array() {
        for node in nodes {
            let version = node["dataset_version"]
                .as_u64()
                .map(|v| v.to_string())
                .unwrap_or_else(|| "-".to_string());
            println!(
                "- {} (type_id={}, rows={}, dataset_version={})",
                node["name"].as_str().unwrap_or_default(),
                node["type_id"].as_u64().unwrap_or(0),
                node["rows"].as_u64().unwrap_or(0),
                version,
            );
            if let Some(description) = node["description"].as_str() {
                println!("  description: {}", description);
            }
            if let Some(instruction) = node["instruction"].as_str() {
                println!("  instruction: {}", instruction);
            }
            if let Some(key_property) = node["key_property"].as_str() {
                println!("  key: {}", key_property);
            }
            if let Some(unique_properties) = node["unique_properties"].as_array()
                && !unique_properties.is_empty()
            {
                let joined = unique_properties
                    .iter()
                    .filter_map(|value| value.as_str())
                    .collect::<Vec<_>>()
                    .join(", ");
                println!("  unique: {}", joined);
            }
            if let Some(outgoing) = node["outgoing_edges"].as_array()
                && !outgoing.is_empty()
            {
                let joined = outgoing
                    .iter()
                    .map(|edge| {
                        format!(
                            "{} -> {}",
                            edge["name"].as_str().unwrap_or_default(),
                            edge["to_type"].as_str().unwrap_or_default()
                        )
                    })
                    .collect::<Vec<_>>()
                    .join(", ");
                println!("  outgoing: {}", joined);
            }
            if let Some(incoming) = node["incoming_edges"].as_array()
                && !incoming.is_empty()
            {
                let joined = incoming
                    .iter()
                    .map(|edge| {
                        format!(
                            "{} <- {}",
                            edge["name"].as_str().unwrap_or_default(),
                            edge["from_type"].as_str().unwrap_or_default()
                        )
                    })
                    .collect::<Vec<_>>()
                    .join(", ");
                println!("  incoming: {}", joined);
            }
            if let Some(props) = node["properties"].as_array() {
                for prop in props {
                    let mut anns: Vec<String> = Vec::new();
                    if prop["key"].as_bool().unwrap_or(false) {
                        anns.push("@key".to_string());
                    }
                    if prop["unique"].as_bool().unwrap_or(false) {
                        anns.push("@unique".to_string());
                    }
                    if prop["index"].as_bool().unwrap_or(false) {
                        anns.push("@index".to_string());
                    }
                    if let Some(source) = prop["embed_source"].as_str() {
                        anns.push(format!("@embed({})", source));
                    }
                    let ann_suffix = if anns.is_empty() {
                        String::new()
                    } else {
                        format!(" {}", anns.join(" "))
                    };
                    println!(
                        "  - {}: {}{}",
                        prop["name"].as_str().unwrap_or_default(),
                        prop["type"].as_str().unwrap_or_default(),
                        ann_suffix
                    );
                    if let Some(description) = prop["description"].as_str() {
                        println!("    description: {}", description);
                    }
                }
            }
        }
    }
    println!();

    println!("Edge Types");
    if let Some(edges) = payload["edges"].as_array() {
        for edge in edges {
            let version = edge["dataset_version"]
                .as_u64()
                .map(|v| v.to_string())
                .unwrap_or_else(|| "-".to_string());
            println!(
                "- {}: {} -> {} (type_id={}, rows={}, dataset_version={})",
                edge["name"].as_str().unwrap_or_default(),
                edge["src_type"].as_str().unwrap_or_default(),
                edge["dst_type"].as_str().unwrap_or_default(),
                edge["type_id"].as_u64().unwrap_or(0),
                edge["rows"].as_u64().unwrap_or(0),
                version,
            );
            if let Some(description) = edge["description"].as_str() {
                println!("  description: {}", description);
            }
            if let Some(instruction) = edge["instruction"].as_str() {
                println!("  instruction: {}", instruction);
            }
            if let Some(endpoint_keys) = edge["endpoint_keys"].as_object() {
                println!(
                    "  endpoint keys: {} -> {}",
                    endpoint_keys
                        .get("src")
                        .and_then(|value| value.as_str())
                        .unwrap_or("-"),
                    endpoint_keys
                        .get("dst")
                        .and_then(|value| value.as_str())
                        .unwrap_or("-")
                );
            }
            if let Some(props) = edge["properties"].as_array() {
                for prop in props {
                    println!(
                        "  - {}: {}",
                        prop["name"].as_str().unwrap_or_default(),
                        prop["type"].as_str().unwrap_or_default()
                    );
                    if let Some(description) = prop["description"].as_str() {
                        println!("    description: {}", description);
                    }
                }
            }
        }
    }
}

#[instrument(fields(db_path = %db_path.display(), format = format))]
pub(crate) async fn cmd_export(db_path: PathBuf, format: &str, json: bool) -> Result<()> {
    let db = Database::open(&db_path).await?;
    let effective_format = if json { "json" } else { format };
    let include_internal_fields = effective_format == "json";
    let rows = build_export_rows(&db, include_internal_fields)?;

    match effective_format {
        "jsonl" => {
            for row in rows {
                println!(
                    "{}",
                    serde_json::to_string(&row).wrap_err("failed to serialize export row")?
                );
            }
        }
        "json" => {
            let out =
                serde_json::to_string_pretty(&rows).wrap_err("failed to serialize export JSON")?;
            println!("{}", out);
        }
        other => return Err(eyre!("unknown format: {} (supported: jsonl, json)", other)),
    }

    Ok(())
}

pub(crate) fn build_export_rows(
    db: &Database,
    include_internal_fields: bool,
) -> Result<Vec<serde_json::Value>> {
    use arrow_array::{Array, UInt64Array};

    let storage = db.snapshot();
    let mut rows = Vec::new();
    let mut node_key_tokens: HashMap<String, HashMap<u64, String>> = HashMap::new();

    for node in db.schema_ir.node_types() {
        let Some(batch) = storage.get_all_nodes(&node.name)? else {
            continue;
        };
        let id_arr = batch
            .column_by_name("id")
            .and_then(|col| col.as_any().downcast_ref::<UInt64Array>())
            .ok_or_else(|| eyre!("node batch '{}' missing UInt64 id column", node.name))?;
        let key_prop = node
            .properties
            .iter()
            .find(|prop| prop.key)
            .map(|prop| prop.name.as_str());
        let key_col = match key_prop {
            Some(prop_name) => {
                let key_idx =
                    node_property_index(batch.schema().as_ref(), prop_name).ok_or_else(|| {
                        eyre!(
                            "node batch '{}' missing @key property '{}'",
                            node.name,
                            prop_name
                        )
                    })?;
                Some((prop_name.to_string(), batch.column(key_idx).clone()))
            }
            None => None,
        };

        let mut key_tokens = HashMap::new();
        for row_idx in 0..batch.num_rows() {
            let id = id_arr.value(row_idx);
            if let Some((prop_name, key_array)) = key_col.as_ref() {
                let key_token = export_key_token(key_array, row_idx, prop_name)?;
                key_tokens.insert(id, key_token);
            }

            let data = export_data_map(&batch, row_idx, &[0]);
            let mut row = serde_json::json!({
                "type": node.name,
                "data": data,
            });
            if include_internal_fields {
                row["id"] = serde_json::Value::Number(id.into());
            }
            rows.push(row);
        }
        if !key_tokens.is_empty() {
            node_key_tokens.insert(node.name.clone(), key_tokens);
        }
    }

    for edge in db.schema_ir.edge_types() {
        let Some(batch) = storage.edge_batch_for_save(&edge.name)? else {
            continue;
        };
        let id_arr = batch
            .column_by_name("id")
            .and_then(|col| col.as_any().downcast_ref::<arrow_array::UInt64Array>())
            .ok_or_else(|| eyre!("edge batch '{}' missing UInt64 id column", edge.name))?;
        let src_arr = batch
            .column_by_name("src")
            .and_then(|col| col.as_any().downcast_ref::<arrow_array::UInt64Array>())
            .ok_or_else(|| eyre!("edge batch '{}' missing UInt64 src column", edge.name))?;
        let dst_arr = batch
            .column_by_name("dst")
            .and_then(|col| col.as_any().downcast_ref::<arrow_array::UInt64Array>())
            .ok_or_else(|| eyre!("edge batch '{}' missing UInt64 dst column", edge.name))?;

        for row_idx in 0..batch.num_rows() {
            let id = id_arr.value(row_idx);
            let src = src_arr.value(row_idx);
            let dst = dst_arr.value(row_idx);
            let from = node_key_tokens
                .get(&edge.src_type_name)
                .and_then(|m| m.get(&src))
                .cloned()
                .ok_or_else(|| {
                    eyre!(
                        "cannot export portable edge '{}': source {} node {} is missing an @key token",
                        edge.name,
                        edge.src_type_name,
                        src
                    )
                })?;
            let to = node_key_tokens
                .get(&edge.dst_type_name)
                .and_then(|m| m.get(&dst))
                .cloned()
                .ok_or_else(|| {
                    eyre!(
                        "cannot export portable edge '{}': destination {} node {} is missing an @key token",
                        edge.name,
                        edge.dst_type_name,
                        dst
                    )
                })?;
            let data = export_data_map(&batch, row_idx, &[0, 1, 2]);

            let mut row = serde_json::json!({
                "edge": edge.name,
                "from": from,
                "to": to,
                "data": data,
            });
            if include_internal_fields {
                row["id"] = serde_json::Value::Number(id.into());
                row["src"] = serde_json::Value::Number(src.into());
                row["dst"] = serde_json::Value::Number(dst.into());
            }
            rows.push(row);
        }
    }

    Ok(rows)
}

fn node_property_index(schema: &arrow_schema::Schema, prop_name: &str) -> Option<usize> {
    schema
        .fields()
        .iter()
        .enumerate()
        .skip(1)
        .find_map(|(idx, field)| (field.name() == prop_name).then_some(idx))
}

fn export_key_token(array: &ArrayRef, row_idx: usize, prop_name: &str) -> Result<String> {
    match nanograph::json_output::array_value_to_json(array, row_idx) {
        serde_json::Value::Null => Err(eyre!("@key property {} cannot be null", prop_name)),
        serde_json::Value::String(value) => Ok(value),
        serde_json::Value::Bool(value) => Ok(value.to_string()),
        serde_json::Value::Number(value) => Ok(value.to_string()),
        other => Err(eyre!(
            "unsupported @key export value for {}: {}",
            prop_name,
            other
        )),
    }
}

fn export_data_map(
    batch: &RecordBatch,
    row_idx: usize,
    excluded_indices: &[usize],
) -> serde_json::Value {
    let excluded = excluded_indices.iter().copied().collect::<HashSet<_>>();
    let mut data = serde_json::Map::new();
    for (col_idx, field) in batch.schema().fields().iter().enumerate() {
        if excluded.contains(&col_idx) {
            continue;
        }
        data.insert(
            field.name().clone(),
            nanograph::json_output::array_value_to_json(batch.column(col_idx), row_idx),
        );
    }
    serde_json::Value::Object(data)
}

fn prop_type_string(prop: &nanograph::schema_ir::PropDef) -> String {
    let base = if prop.enum_values.is_empty() {
        prop.scalar_type.clone()
    } else {
        format!("enum({})", prop.enum_values.join(", "))
    };
    let wrapped = if prop.list {
        format!("[{}]", base)
    } else {
        base
    };
    if prop.nullable {
        format!("{}?", wrapped)
    } else {
        wrapped
    }
}

mod common;

use std::fs;

use common::{ExampleProject, ExampleWorkspace};

fn warning_contains(value: &serde_json::Value, needle: &str) -> bool {
    value["warnings"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|warning| warning.as_str())
        .any(|warning| warning.contains(needle))
}

#[test]
fn changes_still_read_from_wal_when_graph_mirrors_are_missing() {
    let workspace = ExampleWorkspace::copy(ExampleProject::Revops);
    workspace.init();
    workspace.load();

    workspace.delete_file("omni.nano/__graph_commits");
    workspace.delete_file("omni.nano/__graph_changes");

    let changes = workspace.run_ok(&["changes", "--since", "0", "--format", "jsonl"]);
    assert!(changes.stdout.contains("\"db_version\":"));
    assert!(changes.stdout.contains("\"type_name\":\"Signal\""));

    let doctor = workspace.json_value(&["--json", "doctor"]);
    assert_eq!(doctor["status"], "ok");
    assert_eq!(doctor["healthy"], true);
    assert!(warning_contains(
        &doctor,
        "graph mirror tables are missing or empty"
    ));
}

#[test]
fn cleanup_rebuilds_missing_graph_mirrors_end_to_end() {
    let workspace = ExampleWorkspace::copy(ExampleProject::Revops);
    workspace.init();
    workspace.load();

    workspace.delete_file("omni.nano/__graph_commits");
    workspace.delete_file("omni.nano/__graph_changes");
    assert!(!workspace.file("omni.nano/__graph_commits").exists());
    assert!(!workspace.file("omni.nano/__graph_changes").exists());

    let cleanup = workspace.json_value(&[
        "--json",
        "cleanup",
        "--retain-tx-versions",
        "2",
        "--retain-dataset-versions",
        "1",
    ]);
    assert_eq!(cleanup["status"], "ok");
    assert!(workspace.file("omni.nano/__graph_commits").exists());
    assert!(workspace.file("omni.nano/__graph_changes").exists());

    let doctor = workspace.json_value(&["--json", "doctor"]);
    assert_eq!(doctor["status"], "ok");
    assert_eq!(doctor["healthy"], true);
    assert_eq!(
        doctor["warnings"].as_array().map(|warnings| warnings.len()),
        Some(0)
    );
}

#[test]
fn doctor_warns_when_graph_mirrors_are_unreadable() {
    let workspace = ExampleWorkspace::copy(ExampleProject::Revops);
    workspace.init();
    workspace.load();

    workspace.delete_file("omni.nano/__graph_commits");
    workspace.delete_file("omni.nano/__graph_changes");
    fs::write(
        workspace.file("omni.nano/__graph_commits"),
        "not a lance dataset",
    )
    .unwrap();
    fs::write(
        workspace.file("omni.nano/__graph_changes"),
        "not a lance dataset",
    )
    .unwrap();

    let doctor = workspace.json_value(&["--json", "doctor"]);
    assert_eq!(doctor["status"], "ok");
    assert_eq!(doctor["healthy"], true);
    assert!(warning_contains(
        &doctor,
        "graph mirror tables are unreadable"
    ));

    let changes = workspace.run_ok(&["changes", "--since", "0", "--format", "jsonl"]);
    assert!(changes.stdout.contains("\"db_version\":"));
}

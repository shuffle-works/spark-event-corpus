import pytest

from corpus.catalog import append_entry, load_catalog, log_relpath, save_catalog, sha256_of, size_of


def test_sha256_of_matches_known_hash(tmp_path):
    f = tmp_path / "sample.txt"
    f.write_bytes(b"hello world")
    assert sha256_of(f) == (
        "sha256:b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9"
    )


def test_append_entry_writes_and_round_trips(tmp_path):
    catalog_path = tmp_path / "index.json"
    entry = {"id": "test-1", "path": "logs/test-1.ndjson"}
    append_entry(catalog_path, entry)
    assert load_catalog(catalog_path) == [entry]


def test_save_catalog_round_trips_and_leaves_no_temp_file(tmp_path):
    catalog_path = tmp_path / "index.json"
    entries = [{"id": "a", "path": "logs/a.ndjson"}, {"id": "b", "path": "logs/b.ndjson"}]
    save_catalog(catalog_path, entries)
    assert load_catalog(catalog_path) == entries
    assert sorted(p.name for p in tmp_path.iterdir()) == ["index.json"]


def test_save_catalog_overwrite_is_atomic_replace(tmp_path):
    catalog_path = tmp_path / "index.json"
    save_catalog(catalog_path, [{"id": "old"}])
    save_catalog(catalog_path, [{"id": "new"}])
    assert load_catalog(catalog_path) == [{"id": "new"}]
    assert sorted(p.name for p in tmp_path.iterdir()) == ["index.json"]


def test_append_entry_rejects_duplicate_id(tmp_path):
    catalog_path = tmp_path / "index.json"
    append_entry(catalog_path, {"id": "dup", "path": "a"})
    with pytest.raises(ValueError):
        append_entry(catalog_path, {"id": "dup", "path": "b"})


def test_sha256_of_a_directory_changes_with_any_file_or_name(tmp_path):
    d = tmp_path / "eventlog_v2_app"
    d.mkdir()
    (d / "events_1_app").write_bytes(b"one")
    (d / "appstatus_app").write_bytes(b"")
    first = sha256_of(d)
    assert first == sha256_of(d)
    (d / "events_1_app").write_bytes(b"two")
    assert sha256_of(d) != first
    (d / "events_1_app").write_bytes(b"one")
    (d / "appstatus_app").rename(d / "appstatus_app.inprogress")
    assert sha256_of(d) != first


def test_size_of_a_directory_sums_its_files(tmp_path):
    d = tmp_path / "eventlog_v2_app"
    d.mkdir()
    (d / "a").write_bytes(b"123")
    (d / "b").write_bytes(b"45")
    assert size_of(d) == 5


def test_log_relpath_by_layout_and_codec(tmp_path):
    rolling = tmp_path / "eventlog_v2_app-1"
    rolling.mkdir()
    assert log_relpath("eventlog-rolling", rolling) == "logs/eventlog-rolling/eventlog_v2_app-1"
    assert log_relpath("eventlog-zstd", tmp_path / "app-1.zstd") == "logs/eventlog-zstd.zstd"
    assert log_relpath("pairwise-01", tmp_path / "app-1") == "logs/pairwise-01.ndjson"

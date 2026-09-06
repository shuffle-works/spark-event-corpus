import pytest

from corpus.catalog import append_entry, load_catalog, save_catalog, sha256_of


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

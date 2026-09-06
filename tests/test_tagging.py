import json
import re

from corpus.tagging import current_generation_tag, generation_tag_for


def test_current_generation_tag_format():
    assert re.fullmatch(r"v\d{4}-\d{2}-\d{2}", current_generation_tag())


def test_generation_tag_for_missing_catalog_is_fresh_tag(tmp_path):
    assert generation_tag_for(tmp_path / "index.json") == current_generation_tag()


def test_generation_tag_for_empty_catalog_is_fresh_tag(tmp_path):
    catalog_path = tmp_path / "index.json"
    catalog_path.write_text("[]\n")
    assert generation_tag_for(catalog_path) == current_generation_tag()


def test_generation_tag_for_reuses_existing_entrys_tag(tmp_path):
    """A run restarted on a later calendar day must keep stamping the tag its
    earlier invocation already committed, not today's fresh one -- the data
    repo only ever gets that one hand-applied tag."""
    catalog_path = tmp_path / "index.json"
    stale_tag = "v1999-01-01"
    catalog_path.write_text(json.dumps([{"id": "a", "data_repo_tag": stale_tag}]))
    assert stale_tag != current_generation_tag()
    assert generation_tag_for(catalog_path) == stale_tag

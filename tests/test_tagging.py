import re

from corpus.tagging import current_generation_tag


def test_current_generation_tag_format():
    assert re.fullmatch(r"v\d{4}-\d{2}-\d{2}", current_generation_tag())

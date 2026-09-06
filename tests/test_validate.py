import pytest

from corpus.validate import validate_ndjson_event_log, InvalidEventLog


def test_valid_log_returns_line_count(tmp_path):
    f = tmp_path / "log.ndjson"
    f.write_text(
        '{"Event": "SparkListenerApplicationStart"}\n'
        '{"Event": "SparkListenerApplicationEnd"}\n'
    )
    assert validate_ndjson_event_log(f) == 2


def test_malformed_json_raises(tmp_path):
    f = tmp_path / "log.ndjson"
    f.write_text('{"Event": "ok"}\nnot json\n')
    with pytest.raises(InvalidEventLog):
        validate_ndjson_event_log(f)


def test_missing_event_field_raises(tmp_path):
    f = tmp_path / "log.ndjson"
    f.write_text('{"NotEvent": "ok"}\n')
    with pytest.raises(InvalidEventLog):
        validate_ndjson_event_log(f)


def test_empty_file_raises(tmp_path):
    f = tmp_path / "log.ndjson"
    f.write_text("")
    with pytest.raises(InvalidEventLog):
        validate_ndjson_event_log(f)

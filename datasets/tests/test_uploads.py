import importlib
import io
from unittest.mock import Mock

import pytest
import databricks.sdk


@pytest.fixture
def uploader(monkeypatch):
    client = Mock()
    monkeypatch.setattr(databricks.sdk, "WorkspaceClient", lambda: client)
    module = importlib.import_module("datasets.main")
    monkeypatch.setattr(module, "WS", client)
    return module, client


def test_upload_preserves_bytes_and_destination(tmp_path, uploader):
    module, client = uploader
    source = tmp_path / "prices.json"
    source.write_bytes(b'[{"symbol":"AAPL"}]')
    module.upload_files_to_databricks(source, "/Volumes/test/prices.json")
    args, kwargs = client.files.upload.call_args
    assert args[0] == "/Volumes/test/prices.json"
    assert isinstance(args[1], io.BytesIO)
    assert args[1].getvalue() == source.read_bytes()
    assert kwargs == {"overwrite": True}


def test_upload_error_propagates(tmp_path, uploader):
    module, client = uploader
    source = tmp_path / "prices.json"
    source.write_text("[]")
    client.files.upload.side_effect = RuntimeError("upload rejected")
    with pytest.raises(RuntimeError, match="upload rejected"):
        module.upload_files_to_databricks(source, "/Volumes/test/prices.json")


def test_missing_file_fails_without_calling_sdk(tmp_path, uploader):
    module, client = uploader
    with pytest.raises(FileNotFoundError):
        module.upload_files_to_databricks(
            tmp_path / "missing.json", "/Volumes/test/prices.json"
        )
    client.files.upload.assert_not_called()

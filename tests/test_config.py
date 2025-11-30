from pathlib import Path

import pytest

from job_helper.config import LogDir, LogFile, LogPath


def set_monkeypatch_init_context(monkeypatch, tmp_path):
    monkeypatch.setattr(
        "job_helper.config._init_context", lambda: (tmp_path / "pyproject.toml", None)
    )


@pytest.mark.parametrize("input_value", [Path("test/path"), "test/path"])
def test_logpath_factory(tmp_path, monkeypatch, input_value):
    set_monkeypatch_init_context(monkeypatch, tmp_path)
    log_path = LogPath.model_validate(input_value)
    expected_path = (tmp_path / "test/path").resolve()
    assert log_path.get_path() == expected_path
    assert log_path.unified is True


@pytest.mark.parametrize("input_value", [Path("logs"), "logs"])
def test_logdir_factory(tmp_path, monkeypatch, input_value):
    set_monkeypatch_init_context(monkeypatch, tmp_path)
    log_dir = LogDir.model_validate(input_value)
    expected_path = (tmp_path / "logs").resolve()
    assert log_dir.get_path() == expected_path
    assert expected_path.exists()


@pytest.mark.parametrize("input_value", [Path("app.log"), "app.log"])
def test_logfile_factory(tmp_path, monkeypatch, input_value):
    set_monkeypatch_init_context(monkeypatch, tmp_path)
    log_file = LogFile.model_validate(input_value)
    expected_path = (tmp_path / "app.log").resolve()
    assert log_file.get_path() == expected_path
    assert log_file.get_path().parent.exists()


def test_logpath_default_values():
    log_path = LogPath(path=Path("test"))
    assert log_path.unified is True


@pytest.mark.parametrize("unified", [True, False])
def test_logpath_unified_path_resolution(tmp_path, monkeypatch, unified):
    set_monkeypatch_init_context(monkeypatch, tmp_path)
    log_path = LogPath(path=Path("logs/app"), unified=unified)
    if unified:
        expected_path = (tmp_path / "logs/app").resolve()
    else:
        expected_path = Path("logs/app").resolve()
    assert log_path.get_path() == expected_path


def test_logpath_does_not_create_paths():
    log_path = LogPath(path=Path("nonexistent/path"), unified=False)
    assert not log_path.get_path().exists()


@pytest.mark.parametrize(
    "path_str,expected_subpath",
    [
        ("logs/app", "logs/app"),
        ("a/b/c/d/logs", "a/b/c/d/logs"),
    ],
)
def test_logdir_creates_directory(tmp_path, monkeypatch, path_str, expected_subpath):
    set_monkeypatch_init_context(monkeypatch, tmp_path)
    log_dir = LogDir(path=Path(path_str))
    expected_path = (tmp_path / expected_subpath).resolve()
    assert log_dir.get_path() == expected_path
    assert log_dir.get_path().exists()
    assert log_dir.get_path().is_dir()


def test_logdir_non_unified_path(tmp_path):
    test_path = tmp_path / "custom/logs"
    log_dir = LogDir(path=test_path, unified=False)
    assert log_dir.get_path() == test_path.resolve()
    assert log_dir.get_path().exists()
    assert log_dir.get_path().is_dir()


def test_logdir_existing_directory(tmp_path, monkeypatch):
    set_monkeypatch_init_context(monkeypatch, tmp_path)
    existing_dir = tmp_path / "existing"
    existing_dir.mkdir()
    log_dir = LogDir(path=Path("existing"))
    assert log_dir.get_path() == existing_dir.resolve()
    assert log_dir.get_path().exists()


@pytest.mark.parametrize(
    "path_str,expected_subpath",
    [
        ("logs/app.log", "logs/app.log"),
        ("a/b/c/d/app.log", "a/b/c/d/app.log"),
    ],
)
def test_logfile_creates_parent_directory(
    tmp_path, monkeypatch, path_str, expected_subpath
):
    set_monkeypatch_init_context(monkeypatch, tmp_path)
    log_file = LogFile(path=Path(path_str))
    expected_path = (tmp_path / expected_subpath).resolve()
    assert log_file.get_path() == expected_path
    assert not log_file.get_path().exists()


def test_logfile_non_unified_path(tmp_path):
    test_path = tmp_path / "custom/app.log"
    log_file = LogFile(path=test_path, unified=False)
    assert log_file.get_path() == test_path.resolve()
    assert log_file.get_path().parent.exists()
    assert not log_file.get_path().exists()

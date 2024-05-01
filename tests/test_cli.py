import subprocess
import sys
from unittest.mock import patch

import pytest
from job_helper import ArgBase, jhcfg
from job_helper.cli import console_main, init

from tests.utils import MockJhcfg


class SayHello(ArgBase):
    name: str = "stranger"

    def run(self):
        print("Hello,", self.name)


@pytest.fixture
def testing_jhcfg(tmp_path):
    with MockJhcfg(
        cli={"logging_cmd": True, "log_file": tmp_path / "jh_cmd.log"},
        commands={"hello": "tests.test_cli.SayHello"},
    ):
        yield


def test_cli(testing_jhcfg, capsys):
    cmd = ["jh", "hello", "--name", "world", "-", "run"]
    with patch.object(sys, "argv", cmd):
        console_main()
    captured = capsys.readouterr()
    assert captured.out == "Hello, world\n"
    assert captured.err == ""

    # Check log file
    assert " ".join(cmd) in jhcfg.cli.log_file.read_text()


def test_init(tmp_path, monkeypatch, capsys):
    monkeypatch.syspath_prepend(tmp_path)
    monkeypatch.chdir(tmp_path)
    init()
    capsys.readouterr()
    cmd = ["jh", "add-one", "-n", "2", "-", "run"]
    with patch.object(sys, "argv", cmd):
        console_main()
    captured = capsys.readouterr()
    assert captured.out == "3\n"
    assert captured.err == ""

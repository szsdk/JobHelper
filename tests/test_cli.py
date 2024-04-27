import sys
from unittest.mock import patch

import pytest
from job_helper import PDArgBase, jhcfg
from job_helper.cli import console_main

from tests.utils import MockJhcfg


class SayHello(PDArgBase):
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

    # Check log file
    assert " ".join(cmd) in jhcfg.cli.log_file.read_text()

from job_helper import cli, jhcfg

from tests.utils import run_jh, testing_jhcfg


def test_init(tmp_path, monkeypatch, capsys):
    monkeypatch.syspath_prepend(tmp_path)
    monkeypatch.chdir(tmp_path)
    run_jh("jh init")
    capsys.readouterr()
    run_jh("jh add-one -n 2 - run")
    captured = capsys.readouterr()
    assert captured.out == "3\n"
    assert captured.err == ""


def test_tools(tmp_path, testing_jhcfg):
    fn = jhcfg.slurm.log_dir / "test.out"
    cli.tools.log_sh(f"echo 123 >> {fn}")
    assert fn.read_text() == "123\n"
    cli.tools.log_message("hello")
    cli.tools.compress_log(0)

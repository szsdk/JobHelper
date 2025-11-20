import subprocess

from job_helper import cli, jhcfg
from job_helper.project_helper import get_scheduler
from tests.utils import run_jh, slurm_server, testing_jhcfg


def test_init(tmp_path, monkeypatch, capsys, slurm_server):
    monkeypatch.syspath_prepend(tmp_path)
    monkeypatch.chdir(tmp_path)
    run_jh("jh init")
    capsys.readouterr()
    subprocess.run(
        """git init
    git add .
    git commit -m "init"
    """,
        shell=True,
        cwd=tmp_path,
        check=True,
    )
    run_jh("jh project from-config project.yaml run --nodry")


def test_tools(tmp_path, testing_jhcfg):
    fn = get_scheduler().log_dir / "test.out"
    cli.tools.log_sh(f"echo 123 >> {fn}")
    assert fn.read_text() == "123\n"
    cli.tools.log_message("hello")
    cli.tools.compress_log(0)


def test_pipeline(tmp_path, monkeypatch, slurm_server):
    monkeypatch.syspath_prepend(tmp_path)
    monkeypatch.chdir(tmp_path)

    print(tmp_path)
    subprocess.run(
        """
jh init
git init
git add .
git commit -m "init"
python -m fire cli AddOne -n 2 - run
jh project from-config project.yaml run --nodry
jh project-result from-config log/project/1.json - job_states
jh project-result from-config log/project/1.json - job_states -o tt.html
jh tools log-sh "echo 123 >> log/slurm/test.out"
jh tools log-message "hello" warning
jh tools compress-log 0
    """,
        shell=True,
        cwd=tmp_path,
        check=True,
    )

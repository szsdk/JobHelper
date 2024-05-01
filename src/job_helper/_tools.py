import copy
import datetime
import logging
import shlex
import subprocess
import sys
import tarfile
from pathlib import Path

import toml

from .config import JobHelperConfig, jhcfg


def compress_log(dt: float = 24) -> None:
    """
    This function compresses the '.out' and '.sh' files which are not modified more than a certain
    time, `dt` (default=24 hours), in a directory, `log_dir` (default='log/slurm') to a tar.gz file.
    The file name is the current date+time.
    """
    log_dir = jhcfg.slurm.log_dir
    # Get the current date+time
    now = datetime.datetime.now()
    now_str = now.strftime("%Y%m%d_%H%M%S")
    # Set the time threshold
    time_threshold = (now - datetime.timedelta(hours=dt)).timestamp()
    # Get the list of files to be compressed
    files = [f for f in log_dir.glob("*.out") if f.stat().st_mtime < time_threshold]
    files += [f for f in log_dir.glob("*.sh") if f.stat().st_mtime < time_threshold]
    # Compress the files
    if len(files) == 0:
        logging.warning("No files to compress.")
        return
    logging.info(f"Compressing {len(files)} files to {now_str}.tar.gz")
    with tarfile.open(log_dir / f"{now_str}.tar.gz", "w:gz") as tar:
        for file in files:
            tar.add(file)
            file.unlink()


def log_cmd() -> None:
    """
    log the command
    ```bash
    log_sh ls -all
    ```
    """
    jhcfg.cmd_logger.info(shlex.join(sys.argv), extra={"typename": "CMD"})


def log_sh(command: str) -> None:
    """
    Run a shell command and log it if the command succeeds.
    ```
    log_sh ls -all
    ```
    """
    subprocess.run(command, shell=True, check=True)
    jhcfg.cmd_logger.info(command, extra={"typename": "SH"})
    exit()


def log_message(message: str, level: str = "info") -> None:
    """
    Add some logging information to `cmd.log`
    ```
    log_message "hello" warning
    ```
    """
    cmd_logger = jhcfg.cmd_logger
    log_cmds = {
        "info": cmd_logger.info,
        "error": cmd_logger.error,
        "warning": cmd_logger.warning,
    }
    log_cmds[level](message, extra={"typename": "MSG"})


EXAMPLE_CODE = """from job_helper import PDArgBase


class AddOne(PDArgBase):
    "add 1 to num"
    num: int

    def run(self):
        return self.num + 1"""


def init():
    """
    Initialize the project directory.
    """
    cfg: JobHelperConfig = copy.copy(jhcfg)
    cfg.commands = {"add_one": "cli.AddOne"}
    cwd = Path().resolve()
    cfg.repo_watcher.watched_repos = [cwd]
    with open("cli.py", "w") as f:
        print(EXAMPLE_CODE, file=f)

    with open("jh_config.toml", "w") as f:
        print(toml.dumps(cfg.model_dump(mode="json")), file=f)

    if not (cwd / ".git").exists():
        logging.warning(
            "This is not a git repository. It is recommended to use git for version control."
        )

    logging.info("jh_config.toml is created.")
    logging.info("""Try:
    jh --help
    jh add-one --help
    jh add-one -n 1 - run""")


tools = {
    "log_sh": log_sh,
    "log_message": log_message,
    "compress_log": compress_log,
    "init": init,
}

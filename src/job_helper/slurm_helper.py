from __future__ import annotations

import datetime
import logging
import os
import shlex
import subprocess
import sys
import tarfile
from typing import ClassVar, Optional

import pydantic
from pydantic import BaseModel, ConfigDict, Field, computed_field, field_validator

from .config import JobHelperConfig

__all__ = [
    "Slurm",
    "compress_log",
    "log_cmd",
    "log_sh",
    "force_commit",
]


__doc__ = r"""
This module provides a simple interface for submitting jobs to a cluster (Slurm).

It is highly recommended to use this module with the [`fire` package](https://github.com/szsdk/python-fire).

## Usful commands
```bash
fd '' log/jobs -e out --changed-within=1week | fzf --preview 'tail {}'

cat log/cmd.log | fzf
```
"""

_env0 = (  # noqa: E402
    os.environ.copy()
)  # It should be before importing other modules, especially `mpi4py`.


def compress_log(dt: float = 24) -> None:
    """
    This function compresses the '.out' and '.sh' files which are not modified more than a certain
    time, `dt` (default=24 hours), in a directory, `log_dir` (default='log/slurm') to a tar.gz file.
    The file name is the current date+time.
    """
    log_dir = JobHelperConfig.get_instance().job_log_dir
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
    JobHelperConfig.get_instance().cmd_logger.info(
        shlex.join(sys.argv), extra={"typename": "CMD"}
    )


def log_sh() -> None:
    """
    Run a shell command and log it if the command succeeds.
    ```
    log_sh ls -all
    ```
    """
    command = shlex.join(sys.argv[2:])
    subprocess.run(command, shell=True, check=True)
    JobHelperConfig.get_instance().cmd_logger.info(command, extra={"typename": "SH"})
    exit()


def log_message(message: str, level: str = "info") -> None:
    """
    Add some logging information to `cmd.log`
    ```
    log_message "hello" warning
    ```
    """
    cmd_logger = JobHelperConfig.get_instance().cmd_logger
    log_cmds = {
        "info": cmd_logger.info,
        "error": cmd_logger.error,
        "warning": cmd_logger.warning,
    }
    log_cmds[level](message, extra={"typename": "MSG"})


class Slurm(BaseModel):
    """
    This class provides a simple interface for submitting jobs to a cluster (Slurm).
    """

    shell: ClassVar[str] = "/bin/sh"
    sbatch_cmd: ClassVar[str] = "sbatch"
    run_cmd: str
    job_id: Optional[int] = None
    config: dict[str, str] = Field(default_factory=dict)

    @field_validator("config")
    @classmethod
    def default_and_replace_underscore(cls, v):
        v = {k.replace("_", "-"): v for k, v in v.items()}
        if "output" not in v:
            jhcfg = JobHelperConfig.get_instance()
            v["output"] = f"{jhcfg.job_log_dir}/%j.out"
        return v

    def set_slurm(self, **kwargs: str) -> Slurm:
        self.config.update({k.replace("_", "-"): v for k, v in kwargs.items()})
        return self

    @computed_field
    def script(self) -> str:
        """
        Generate the script to be submitted to the cluster.
        """
        cmds = [f"#!{self.shell}"]
        for k, v in self.config.items():
            if v is None:
                continue
            cmds.append(f"#SBATCH --{k:<19} {v}")
        cmds.append(self.run_cmd)
        return "\n".join(cmds)

    def sbatch(self, dry: bool = True, save_script: bool = True) -> Slurm:
        """
        Submit the job to the cluster.
        If `dry` is True, it only prints the script. Otherwise (--nodry), it submits the job.
        """
        cfg = JobHelperConfig.get_instance()
        slurm_script = "\n".join([f'{self.sbatch_cmd} << "EOF"', self.script, "EOF"])
        print(self.script)
        if dry:
            logging.info("It is a dry run.")
            return self

        result = subprocess.run(
            slurm_script, shell=True, stdout=subprocess.PIPE, env=_env0
        )
        success_msg = "Submitted batch job"
        stdout = result.stdout.decode("utf-8").strip()
        if success_msg not in stdout:
            logging.error(result.stderr)
            sys.exit(1)
        self.job_id = int(stdout.split(" ")[3])
        log_cmd()
        cfg.cmd_logger.info(stdout)
        if save_script:
            with (cfg.job_log_dir / f"{self.job_id}_slurm.sh").open("w") as fp:
                print(self.script, file=fp)
        return self

    def __str__(self) -> str:
        return f"{type(self).__name__}(job id: {self.job_id})"


def git_status(scope: str = "folder") -> list[tuple[str, str]]:
    """
    Check the Git status of the current folder or the whole repository.
    `scope` can be either "folder" or "repository".
    """

    # Run the 'git status . -s' command
    result = subprocess.run(
        ["git", "status", "-s", "."] if scope == "folder" else ["git", "status", "-s"],
        capture_output=True,
        text=True,
    )
    ans = []
    for line in result.stdout.strip().split("\n"):
        line = line.strip()
        if len(line) > 0:
            ans.append((line[:2], line[2:].strip()))
    return ans


def git_commit_hash() -> str:
    """
    Get the commit hash of the current commit.
    """
    result = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        capture_output=True,
        text=True,
    )
    return result.stdout.strip()


def force_commit(scope: str = "folder") -> None:
    """
    Make sure all changes in the current folder or the whole repository are committed.
    """
    uncommitted = git_status(scope)
    if uncommitted == []:
        JobHelperConfig.get_instance().cmd_logger.info(
            f"commit: {git_commit_hash()}", extra={"typename": "GIT"}
        )
        return
    logging.warning("The following files are not committed:")
    for status, file in uncommitted:
        logging.warning(f"{status} {file}")
    if input("do you want to commit all changes? (Y/n)") in ["Y", "y", "yes", ""]:
        subprocess.run(["git", "add", "."], capture_output=True, text=True)
        # Use the current time as the commit message
        commit_message = f"Auto commit at {datetime.datetime.now()}"
        os.system(f"git commit -m '{commit_message}'")
        logging.info(f"Commit message: {commit_message}")
    raise Exception(f"Some files are not committed in the {scope}!")

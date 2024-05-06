from pathlib import Path

from job_helper import JobArgBase, Slurm


class AddOne(JobArgBase):
    "add 1 to num"

    num: int

    def run(self):
        return self.num + 1

    def script(self):
        return f"""
cd {Path(__file__).parent}
jh add-one from-base64 {self.to_base64()} - run"""

    def slurm(self):
        return Slurm(run_cmd=self.script())

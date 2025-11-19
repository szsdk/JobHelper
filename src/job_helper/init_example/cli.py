from job_helper import JobArgBase


class AddOne(JobArgBase):
    "add 1 to num"

    num: int

    def run(self):
        return self.num + 1

    # The running script can be modified
    # def script(self):
    #     from pathlib import Path
    #     return "\n".join([f"cd {Path(__file__).parent}", JobArgBase.script(self)])

    # This is the innver machenism to get a Slrum object. There is another chance to modify the
    # script here.
    # def slurm(self):
    #     from job_helper import Slurm
    #     return Slurm(run_cmd=self.script())

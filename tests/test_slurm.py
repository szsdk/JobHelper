from job_helper import Slurm, jhcfg
from job_helper.project_helper import get_scheduler

from tests.utils import slurm_server, testing_jhcfg


def test_s(tmpdir, testing_jhcfg, slurm_server):
    fn = tmpdir / "a.txt"
    s = Slurm(run_cmd=f"echo 'hhh' > {fn}")
    get_scheduler().sbatch(s, dry=False)
    slurm_server.complete_all()
    assert s.job_id == 1
    with fn.open() as f:
        assert f.read() == "hhh\n"

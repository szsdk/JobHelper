from job_helper import Slurm

from tests.fake_slurm import SlurmServer
from tests.utils import testing_jhcfg


def test_s(tmpdir, testing_jhcfg):
    fn = tmpdir / "a.txt"
    s = Slurm(run_cmd=f"echo 'hhh' > {fn}")
    with SlurmServer():
        s.sbatch(dry=False)
    assert s.job_id == 1
    with fn.open() as f:
        assert f.read() == "hhh\n"

from job_helper import Slurm

from tests.fake_slurm import SlurmServer


def test_s(tmpdir):
    fn = tmpdir / "a.txt"
    s = Slurm(run_cmd=f"echo 'hhh' > {fn}")
    with SlurmServer():
        s.sbatch(dry=False)
    assert s.job_id == 1
    with fn.open() as f:
        assert f.read() == "hhh\n"

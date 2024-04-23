from unittest.mock import patch

from job_helper import Slurm
from tests.fake_slurm import SlurmServer


@patch("job_helper.Slurm.sbatch_cmd", "python tests/fake_slurm.py client")
def test_s(tmpdir):
    fn = tmpdir / "a.txt"
    s = Slurm(f"echo 'hhh' > {fn}")
    with SlurmServer():
        s.sbatch(dry=False)
    assert s.job_id == 1
    with fn.open() as f:
        assert f.read() == "hhh\n"

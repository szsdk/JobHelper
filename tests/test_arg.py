from pathlib import Path

import numpy as np
import yaml
from job_helper import PDArgBase, Slurm


class GenerateDataArg(PDArgBase):
    count: int
    output_fn: str

    def run(self):
        with open(self.output_fn, "w") as f:
            for i in range(self.count):
                print(i, file=f)

    def slurm(self):
        return Slurm(
            run_cmd=f"""
# {self}
export PYTHONPATH={Path(__file__).parent.parent}
python {__file__} {type(self).__name__} from_base64 '{self.to_base64()}' - run
        """
        )


class SumDataArg(PDArgBase):
    input_fn: str
    output_fn: str

    def run(self):
        data = np.loadtxt(self.input_fn, dtype=int)
        with open(self.output_fn, "w") as f:
            print(data.sum(), file=f)

    def slurm(self):
        return Slurm(
            run_cmd=f"""
# {self}
export PYTHONPATH={Path(__file__).parent.parent}
python {__file__} {type(self).__name__} from_base64 '{self.to_base64()}' - run
        """
        )


def test_PDArg(tmpdir):
    arg = GenerateDataArg(count=100, output_fn=str(tmpdir / "c.txt"))
    cfg_fn = tmpdir / "generate_data.yaml"
    with cfg_fn.open("w") as f:
        print(yaml.dump(arg.model_dump()), file=f)
    assert GenerateDataArg.from_config(Path(cfg_fn)) == arg
    arg.run()
    np.testing.assert_array_equal(np.loadtxt(arg.output_fn, dtype=int), np.arange(100))
    assert GenerateDataArg.from_base64(arg.to_base64()) == arg
    arg.slurm()

    sum_arg = SumDataArg(input_fn=arg.output_fn, output_fn=str(tmpdir / "sum.txt"))
    sum_arg.run()
    assert np.loadtxt(sum_arg.output_fn, dtype=int) == np.arange(100).sum()
    sum_arg.slurm()


if __name__ == "__main__":
    import fire

    fire.Fire({"GenerateDataArg": GenerateDataArg, "SumDataArg": SumDataArg})

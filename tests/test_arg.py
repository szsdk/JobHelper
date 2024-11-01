from pathlib import Path

import yaml
from pydantic import BaseModel

from job_helper import ArgBase
from tests.example_cmds import GenerateDataArg, SumDataArg


class A(BaseModel):
    a: int
    b: str


class B(A, ArgBase):
    c: float = 3


def test_inherit():
    b = B(a=1, b="b", c=3.4)
    assert b.model_dump() == {"a": 1, "b": "b", "c": 3.4}


def test_Arg(tmpdir):
    arg = GenerateDataArg(count=100, output_fn=str(tmpdir / "c.txt"))
    cfg_fn = tmpdir / "generate_data.yaml"
    with cfg_fn.open("w") as f:
        print(yaml.dump(arg.model_dump()), file=f)
    assert GenerateDataArg.from_config(Path(cfg_fn)) == arg
    arg.run()
    with open(arg.output_fn, "r") as f:
        data = f.read().split()
        data = list(map(int, data))

    expected_data = list(range(100))

    assert (
        data == expected_data
    ), f"Data in {arg.output_fn} does not match expected range."
    assert GenerateDataArg.from_base64(arg.to_base64()) == arg
    arg.script()

    sum_arg = SumDataArg(input_fn=arg.output_fn, output_fn=str(tmpdir / "sum.txt"))
    sum_arg.run()
    with open(sum_arg.output_fn, "r") as f:
        output_data = int(f.read().strip())

    expected_sum = sum(range(100))

    assert (
        output_data == expected_sum
    ), f"Sum in {sum_arg.output_fn} does not match expected sum."
    sum_arg.script()

from pathlib import Path

import numpy as np
import yaml

from tests.example_cmds import GenerateDataArg, SumDataArg


def test_Arg(tmpdir):
    arg = GenerateDataArg(count=100, output_fn=str(tmpdir / "c.txt"))
    cfg_fn = tmpdir / "generate_data.yaml"
    with cfg_fn.open("w") as f:
        print(yaml.dump(arg.model_dump()), file=f)
    assert GenerateDataArg.from_config(Path(cfg_fn)) == arg
    arg.run()
    np.testing.assert_array_equal(np.loadtxt(arg.output_fn, dtype=int), np.arange(100))
    assert GenerateDataArg.from_base64(arg.to_base64()) == arg
    arg.script()

    sum_arg = SumDataArg(input_fn=arg.output_fn, output_fn=str(tmpdir / "sum.txt"))
    sum_arg.run()
    assert np.loadtxt(sum_arg.output_fn, dtype=int) == np.arange(100).sum()
    sum_arg.script()

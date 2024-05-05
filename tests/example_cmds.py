import logging
from pathlib import Path

import numpy as np
from job_helper import ArgBase


class GenerateDataArg(ArgBase):
    count: int
    output_fn: str

    def run(self):
        logging.info(f"Generating data to {self.output_fn}")
        with open(self.output_fn, "w") as f:
            for i in range(self.count):
                print(i, file=f)

    def script(self):
        return f"""
# {self}
export PYTHONPATH={Path(__file__).parent.parent}
python {__file__} {type(self).__name__} from_base64 '{self.to_base64()}' - run
        """


class SumDataArg(ArgBase):
    input_fn: str
    output_fn: str

    def run(self):
        logging.info(f"Summing data from {self.input_fn}")
        data = np.loadtxt(self.input_fn, dtype=int)
        logging.info(f"writting sum to {self.output_fn}")
        with open(self.output_fn, "w") as f:
            print(data.sum(), file=f)

    def script(self):
        return f"""
# {self}
export PYTHONPATH={Path(__file__).parent.parent}
python {__file__} {type(self).__name__} from_base64 '{self.to_base64()}' - run"""


if __name__ == "__main__":
    import fire

    fire.Fire({"GenerateDataArg": GenerateDataArg, "SumDataArg": SumDataArg})

import logging
from pathlib import Path

from job_helper import JobArgBase


class GenerateDataArg(JobArgBase):
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


class SumDataArg(JobArgBase):
    input_fn: str
    output_fn: str

    def run(self):
        logging.info(f"Summing data from {self.input_fn}")
        with open(self.input_fn, "r") as f:
            data = f.read().split()
            data = list(map(int, data))
        logging.info(f"writting sum to {self.output_fn}")
        with open(self.output_fn, "w") as f:
            print(sum(data), file=f)

    def script(self):
        return f"""
# {self}
export PYTHONPATH={Path(__file__).parent.parent}
python {__file__} {type(self).__name__} from_base64 '{self.to_base64()}' - run"""


if __name__ == "__main__":
    import fire

    fire.Fire({"GenerateDataArg": GenerateDataArg, "SumDataArg": SumDataArg})

# Convert your script to a `JobArgBase` class

Suppose you start with a simple Python script that performs an addition:

``` python
a = 1
b = 3
print(a + b)
```

You can convert this script into a `job_helper.JobArgBase` class with
just a few steps:

1.  Identify the key input arguments---here, `a` and `b`.
2.  Turn them into attributes of a new class.
3.  Move the rest of your script into the class's `run()` method.

``` python
from job_helper import JobArgBase

class Args(JobArgBase):
    a: int = 1
    b: int = 3

    def run(self):
        print(self.a + self.b)

if __name__ == "__main__":
    Args().run()
```

That's it! You've successfully created a job-argument class. This script
behaves exactly like the original one.
As a general recommendation---especially when you are getting
started---use Python's built-in types such as `int`, `float`, and `str`,
since they're easy to serialize to other formats.

------------------------------------------------------------------------

Now for something more interesting. Suppose your `Args` class lives in a
file named `add.py`.\
You can run:

``` bash
python -m fire add Args
```

This gives you:

    a=1 b=3

From this output, you can infer how the `Args` class would be
instantiated:

``` python
Args(a=1, b=3)
```

------------------------------------------------------------------------

Our eventual goal is to run our function on an HPC cluster through a
scheduler such as Slurm.\
To do this, we need to provide extra information to the job scheduler.
You can collect this information in a dictionary. The only required key
is `dependency`, whose value must be a list of job names or `"START"`.

Next, we combine the function class (`Args`) and the scheduler
information to produce a project configuration file, which is then used
to submit jobs to the cluster. The configuration file can be written in
YAML, JSON, or TOML. Here, we use YAML as an example.

Here is the `gen_project.py` script:

``` python
import yaml
from add import Args

args = Args(a=1, b=3)

jobs = {}
job_name = "add"
jobs[job_name] = {
    "command": "add.Args",
    "config": args.model_dump(),
    "job_preamble": {"dependency": ["START"]},
}

with open("project.yaml", "w") as yaml_file:
    yaml.dump({"jobs": jobs}, yaml_file)
```

Running this script generates a `project.yaml` file like this:

``` yaml
jobs:
  add:
    command: add.Args
    config:
      a: 1
      b: 3
    job_preamble:
      dependency:
      - START
```

You can load this configuration file and perform a dry submission (the
default behavior) with:

``` bash
jh project from-config project.yaml - run
```

------------------------------------------------------------------------

Now for the real magic.

Suppose you spent two nights writing a complex script called `myfunc.py`.
You shouldn't waste time manually converting it---you deserve a cup of
coffee! Before you go get one, just open your LLM CLI and paste the 
following prompt:

    According to @tutorial.md, help me convert @myfunc.py to my_args.py and gen_project.py. Put these two files in a folder named "MyFunc".

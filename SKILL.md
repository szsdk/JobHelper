---
name: job-arg-base
description: >
  Convert Python scripts into JobArgBase classes for HPC/Slurm job scheduling.
  Use this skill whenever the user wants to convert a Python script to a
  job_helper.JobArgBase class, generate a project config YAML/JSON/TOML for
  cluster submission, or structure arguments for Slurm/HPC workflows. Also
  trigger when the user mentions job_helper, JobArgBase, gen_project.py,
  project.yaml, or HPC job scheduling. If the user pastes a Python script and
  asks to "convert it", "prepare it for the cluster", or "make it schedulable",
  use this skill.
---

# JobArgBase Conversion Skill

Converts plain Python scripts into `job_helper.JobArgBase` classes and generates
the corresponding project config file for HPC cluster submission (e.g. Slurm).

---

## Step 1 — Convert the script to a `JobArgBase` class

Given a plain Python script, produce a new file (e.g. `my_args.py`) following this pattern:

1. **Identify input arguments** — variables set near the top of the script that act as parameters.
2. **Create a class** inheriting from `JobArgBase` with those variables as typed class attributes.
3. **Move the logic** into a `run()` method, replacing bare variable names with `self.<name>`.
4. **Add the entry point** guard.

### Example

**Before (`add.py`):**
```python
a = 1
b = 3
print(a + b)
```

**After (`add.py`):**
```python
from job_helper import JobArgBase

class Args(JobArgBase):
    a: int = 1
    b: int = 3

    def run(self):
        print(self.a + self.b)

if __name__ == "__main__":
    Args().run()
```

> **Tip:** Prefer primitive types (`int`, `float`, `str`, `bool`) for attributes — they serialize cleanly to YAML/JSON/TOML.

---

## Step 2 — Verify with `fire`

If the class lives in `add.py`, the user can inspect it with:

```bash
python -m fire add Args
```

Expected output shows the current defaults, e.g.:
```
a=1 b=3
```

This confirms the class can be instantiated as `Args(a=1, b=3)`.

---

## Step 3 — Generate the project config (`gen_project.py`)

Create a `gen_project.py` script that:
- Imports the `Args` class
- Instantiates it with desired parameter values
- Builds a `jobs` dict with `command`, `config`, and `job_preamble`
- Writes a `project.yaml` (or `.json` / `.toml`) config file

### Template

```python
import yaml
from add import Args

args = Args(a=1, b=3)

jobs = {}
job_name = "add"
jobs[job_name] = {
    "command": "add.Args",           # module.ClassName
    "config": args.model_dump(),     # serialized arguments
    "job_preamble": {
        "dependency": ["START"],     # required key; list of job names or "START"
    },
}

with open("project.yaml", "w") as yaml_file:
    yaml.dump({"jobs": jobs}, yaml_file)
```

### Resulting `project.yaml`

```yaml
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

**Key rules for `job_preamble`:**
- `dependency` is **required**.
- Value is a list of job names the current job depends on, or `["START"]` if it has no dependencies.

---

## Step 4 — Dry-run submission

Load the config and perform a dry run (default behavior):

```bash
jh project from-config project.yaml - run
```

---

## File layout convention

When converting a script called `myfunc.py`, place outputs in a named folder:

```
MyFunc/
├── my_args.py       # the JobArgBase class
└── gen_project.py   # project config generator
```

---

## Checklist

- [ ] All input parameters are typed class attributes with defaults
- [ ] Logic is inside `run(self)`
- [ ] `command` in config uses `module.ClassName` format
- [ ] `job_preamble` contains `dependency` key
- [ ] Output files are placed in a dedicated folder

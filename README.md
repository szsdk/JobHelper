# JobHelper

JobHelper helps you run Python scripts on high-performance computing (HPC) clusters more easily. It solves two main problems:

## Problems JobHelper Solves

### 1. Managing Many Parameters
When running scientific code, you often need to handle many input parameters. Typing them all on the command line is tedious and error-prone. JobHelper lets you store parameters in configuration files (YAML, JSON, or TOML) and manage them easily.

### 2. Running Jobs in Order
Many workflows need multiple programs to run in a specific order, where some jobs depend on others finishing first. Job schedulers like Slurm support this, but you need to track job IDs manually. JobHelper automates this by managing the dependency graph for you.

## How JobHelper Works

### `JobArgBase` Class
Convert your Python scripts into a `JobArgBase` class. This gives you:
- Automatic saving/loading of parameters to configuration files
- Instant command-line interface (using `python-fire`)
- Easy parameter management

### `jh project` Command
Put all your job configurations in one file, including which jobs depend on others. Then use `jh project` to submit all jobs to the cluster in the correct order automatically.

## Quick Start

To get started with JobHelper, first create a new project:
Run it with:
```bash
mkdir my_project
cd my_project
jh init
```

Then you will see a sample project structure created for you. It includes the following files:
```
cli.py  
log/
project.yaml
pyproject.toml
```

You can use the following command to take a look at the jobs to be run:
```bash
jh project from-config project.yaml - run
```

If everything looks good, submit the jobs to the cluster with:
```bash
jh project from-config project.yaml - run --nodry
```


### Scale up Existing Scripts with JobHelper

Take a look at `tutorial/tutorial.md` to learn how to scale existing programs / scripts in HPC with
`JobHelper`. Nowadays, large language models (LLMs) can help you convert your existing scripts to
use `JobHelper` quickly and easily.

### Full Tutorial

For a complete hands-on tutorial that covers all the details, see [tutorial/tutorial.md](tutorial/tutorial.md).

## Installation

```bash
pip install git+https://github.com/szsdk/jobhelper.git
```

## Main Commands

- `jh init` - Set up a new project directory
- `jh project from-config <file>` - Load and run jobs from a configuration file
- `jh server` - Start a web server to view project results
- `jh viewer` - View a project file interactively
- `jh config` - Display JobHelper configuration
- `jh tools` - Additional utility commands

For detailed help on any command, run `jh <command> --help`.

## License

MIT

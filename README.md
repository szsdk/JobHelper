# JobHelper

The **JobHelper** package is designed to streamline two common challenges encountered when working with high-performance computing clusters, particularly in scientific computing:

1. **Complex Parameter Management:** Scientific computing applications often require numerous parameters, making command-line argument handling cumbersome and error-prone. Configuration files offer a more readable and maintainable approach, but managing these in detail can still be tedious. For example, when scanning a parameter space, the only changes between runs might be a single parameter or just the input and output file names.

2. **Managing Job Dependencies:** Many computational workflows involve multiple programs with complex dependencies. Although job schedulers support dependency management, the job IDs required to set these dependencies are unknown until submission. Tracking and managing job scripts manually can quickly become overwhelming.

**JobHelper** provides two key solutions:

1. **Configuration Management with `JobArgBase`:** The `JobArgBase` class lets you define a configuration class with attributes that can be easily read from or saved to a configuration file. Additionally, with the `python-fire` package, you can generate a command-line interface instantly, making it easier to manage parameters.

2. **Dependency Management with `jh project`:** By consolidating all configurations for different jobs into a single configuration file, you can manage parameters and the **dependency graph** between jobs from a central location. The `jh project` command then facilitates submitting all jobs in the correct order, minimizing manual intervention.

# gwf-failed-targets

A GWF plugin for logging extended Slurm accounting records of failed targets in a workflow.

## Usage

```shell
Usage: gwf failed-targets [OPTIONS]

  Log records of failed targets.

Options:
  -f, --log-path PATH  Output file path for extended accounting records of
                       failed targets. If not provided, error records will be
                       displayed in a table format on the standard output
                       (stdout).
  --help               Show this message and exit.
```

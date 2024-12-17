# gwf-failed-targets

A GWF plugin for logging extended Slurm accounting records of failed targets in a workflow.

## Installation

Install using conda:

```shell
conda install moma-auh::gwf-failed-targets
```

Or install using pip:

```shell
pip install git+https://github.com/MOMA-AUH/gwf-failed-targets.git
```

## Usage

```raw
Usage: gwf failed-targets [OPTIONS]

  Log records of failed targets.

Options:
  -f, --log-path PATH  Output file path for extended accounting records of
                       failed targets. If not provided, the records will be
                       displayed in a table format on the standard output
                       (stdout).
  --help               Show this message and exit.
```

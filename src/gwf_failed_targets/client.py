import click
from gwf import Workflow
from gwf.backends import create_backend
from gwf.core import (
    CachedFilesystem,
    Context,
    Graph,
    Status,
    Target,
    get_spec_hashes,
    pass_context,
)
from gwf.scheduling import get_status_map
from pathlib import Path
from typing import Dict

from .restart import restart_targets
from .slurm import SlurmAccounting


@click.command()
@click.option(
    "-f",
    "--log-path",
    type=click.Path(path_type=Path),
    help="""Output file path for extended accounting records of failed targets. If not provided,
            the records will be displayed in a table format on the standard output (stdout).""",
)
@click.option(
    "-r",
    "--restart",
    is_flag=True,
    default=False,
    help="""Restart failed targets and their dependents. Only targets with the following failure types
            are restartet: [Timeout, OutOfMemory, FileSystem].""",
)
@click.option(
    "-m",
    "--multiplier",
    type=float,
    default=2.0,
    help="""Multiplicative factor to scale resource options of failed targets. Walltime and memory is
    multiplied for Timeout and OutOfMemory failures, respectively.""",
)
@pass_context
def failed_targets(
    context: Context,
    log_path: Path,
    restart: bool,
    multiplier: float,
):
    """Log records of failed targets."""
    workflow: Workflow = Workflow.from_context(ctx=context)

    fs = CachedFilesystem()
    graph = Graph.from_targets(targets=workflow.targets, fs=fs)

    with create_backend(
        name=context.backend,
        working_dir=context.working_dir,
        config=context.config,
    ) as backend:
        with get_spec_hashes(
            working_dir=context.working_dir,
            config=context.config,
        ) as spec_hashes:

            status_map: Dict[Target, Status] = get_status_map(
                graph=graph,
                fs=fs,
                backend=backend,
                spec_hashes=spec_hashes,
            )

            failed_targets = [
                target
                for target, status in status_map.items()
                if status == Status.FAILED
            ]

            accounting = SlurmAccounting(
                context=context,
                targets=failed_targets,
            )

            if log_path is not None:
                accounting.to_file(path=log_path)
            else:
                accounting.to_stdout()

            if restart:
                restart_targets(
                    targets=workflow.targets,
                    failure_map=accounting.failure_map,
                    multiplier=multiplier,
                    fs=fs,
                    spec_hashes=spec_hashes,
                    backend=backend,
                )

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

from gwf_failed_targets.slurm import SlurmAccounting


@click.command()
@click.option(
    "-f",
    "--log-path",
    type=click.Path(path_type=Path),
    help="""Output file path for extended accounting records of failed targets. If not provided,
            error records will be displayed in a table format on the standard output (stdout).""",
)
@pass_context
def failed_targets(
    context: Context,
    log_path: Path,
):
    """Log records of failed targets."""
    workflow: Workflow = Workflow.from_context(ctx=context)
    fs = CachedFilesystem()
    graph = Graph.from_targets(targets=workflow.targets, fs=fs)
    spec_hashes = get_spec_hashes(
        working_dir=context.working_dir,
        config=context.config,
    )
    backend = create_backend(
        name=context.backend,
        working_dir=context.working_dir,
        config=context.config,
    )
    status_map: Dict[Target, Status] = get_status_map(
        graph=graph,
        fs=fs,
        backend=backend,
        spec_hashes=spec_hashes,
    )

    failed_targets = [
        target for target, status in status_map.items() if status == Status.FAILED
    ]

    accounting = SlurmAccounting(
        context=context,
        targets=failed_targets,
    )

    if log_path is not None:
        accounting.to_file(path=log_path)
    else:
        accounting.to_stdout()

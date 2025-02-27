import re
from datetime import timedelta
from gwf.core import CachedFilesystem, Graph, Target, FileSpecHashes, NoopSpecHashes
from gwf.scheduling import submit_workflow
from typing import Any

from .utilities import FailureType


MEMORY_REGEX = re.compile(r"(?P<size>\d+)(?P<unit>[a-zA-Z]+)")
WALLTIME_REGEX = re.compile(
    r"^((?P<days>\d+)-)?(?P<hours>\d+):(?P<minutes>\d+):(?P<seconds>\d+)$"
)


def parse_walltime(walltime: str) -> timedelta:
    """
    Parses a walltime string and returns a timedelta object.

    Args:
        walltime (str): The walltime string to parse.

    Returns:
        timedelta: A timedelta object representing the parsed walltime.

    Raises:
        ValueError: If the walltime string is in an invalid format.
    """
    parts = WALLTIME_REGEX.match(walltime)

    if not parts:
        raise ValueError(f"Invalid walltime format: {walltime}")

    parts = parts.groupdict()
    time_params = {}
    for name, param in parts.items():
        if param:
            time_params[name] = int(param)

    return timedelta(**time_params)


def format_walltime(walltime: timedelta) -> str:
    """
    Formats a timedelta object into a string representing the walltime.

    The format will be "D-HH:MM:SS" if the timedelta includes days,
    otherwise "HH:MM:SS".

    Args:
        walltime (timedelta): The timedelta object to format.

    Returns:
        str: The formatted walltime string.
    """
    days = walltime.days

    seconds = walltime.seconds
    hours, seconds = divmod(seconds, 3600)
    minutes, seconds = divmod(seconds, 60)

    if days > 0:
        return f"{days}-{hours:02d}:{minutes:02d}:{seconds:02d}"
    return f"{hours:02d}:{minutes:02d}:{seconds:02d}"


def modify_walltime(walltime: str, multiplier: float) -> str:
    """
    Modify the given walltime by multiplying it with the specified multiplier.

    Args:
        walltime (str): The original walltime.
        multiplier (float): The factor by which to multiply the walltime.

    Returns:
        str: The modified walltime.
    """
    walltime_td = parse_walltime(walltime)
    walltime_td *= multiplier
    walltime_str = format_walltime(walltime_td)

    return walltime_str


def modify_memory(memory: str, multiplier: float) -> str:
    """
    Modify the memory size by a given multiplier.

    Args:
        memory (str): The memory size as a string.
        multiplier (float): The multiplier to apply to the memory size.

    Returns:
        str: The modified memory size as a string with the same unit.

    Raises:
        ValueError: If the memory string is in an invalid format.
    """
    parts = MEMORY_REGEX.match(memory)

    if not parts:
        raise ValueError(f"Invalid memory format: {memory}")

    size, unit = parts.groups()
    size = int(size) * multiplier

    return f"{size:.0f}{unit}"


def update_target_options(
    targets: dict[str, Target],
    failure_map: dict[Target, FailureType],
    multiplier: float,
) -> dict[str, Target]:
    """
    Updates the options of targets based on their failure types and a given multiplier.

    Args:
        targets (dict[str, Target]): A dictionary of target names to Target objects.
        failure_map (dict[Target, FailureType]): A dictionary mapping Target objects to their corresponding FailureType.
        multiplier (float): A multiplier used to adjust resources of the failed targets.

    Returns:
        dict[str, Target]: The updated dictionary of target names to Target objects with modified options.
    """
    for target, failure in failure_map.items():
        match failure:
            case FailureType.OutOfMemory:
                target.options["memory"] = modify_memory(
                    memory=target.options["memory"],
                    multiplier=multiplier,
                )
            case FailureType.Timeout:
                target.options["walltime"] = modify_walltime(
                    walltime=target.options["walltime"],
                    multiplier=multiplier,
                )
            case _:
                continue

        targets[target.name] = target

    return targets


def get_restartable_targets(
    dependents: dict[Target, set[Target]],
    failure_map: dict[Target, FailureType],
) -> set[Target]:
    """
    Determine which targets are restartable based on their failure types and dependencies.

    This function analyzes the given failure map and dependents to identify targets that can be restarted.
    Targets that failed due to Timeout, OutOfMemory, or FileSystem are considered restartable, along with their dependents.
    Targets that failed due to other reasons are not restartable, and their dependents are also marked as not restartable.

    Args:
        dependents (dict[Target, set[Target]]): A dictionary mapping each target to its set of dependent targets.
        failure_map (dict[Target, FailureType]): A dictionary mapping each target to its failure type.

    Returns:
        set[Target]: A set of target objects that are restartable.
    """

    def add_dependents(target: Target, target_set: set[str]) -> None:
        target_dependents = dependents.get(target, set())

        for dependent in target_dependents:
            target_set.add(dependent)

            add_dependents(dependent, target_set)

    restartable, not_restartable = set(), set()

    for target, failure in failure_map.items():
        match failure:
            case FailureType.Timeout | FailureType.OutOfMemory | FailureType.FileSystem:
                restartable.add(target)
                add_dependents(target=target, target_set=restartable)
            case _:
                not_restartable.add(target)
                add_dependents(target=target, target_set=not_restartable)

    restartable -= not_restartable

    return restartable


def restart_targets(
    targets: dict[str, Target],
    failure_map: dict[Target, FailureType],
    multiplier: float,
    fs: CachedFilesystem,
    spec_hashes: FileSpecHashes | NoopSpecHashes,
    backend: Any,
) -> None:
    """
    Restart failed targets and their dependents in a workflow.

    This function updates the options for the given targets based on the failure map and multiplier,
    constructs a graph from the updated targets, identifies the restartable targets, and submits
    the workflow for execution.

    Args:
        targets (dict[str, Target]): A dictionary mapping target names to Target objects.
        failure_map (dict[Target, FailureType]): A dictionary mapping Target objects to their respective failure types.
        multiplier (float): A multiplier used to adjust target options.
        fs (CachedFilesystem): A filesystem object used for caching.
        spec_hashes (FileSpecHashes | NoopSpecHashes): An object representing file specification hashes.
        backend (Any): The backend used for workflow execution.

    Returns:
        None
    """
    targets = update_target_options(
        targets=targets,
        failure_map=failure_map,
        multiplier=multiplier,
    )
    graph = Graph.from_targets(targets=targets, fs=fs)

    endpoints = get_restartable_targets(
        dependents=graph.dependents,
        failure_map=failure_map,
    )

    submit_workflow(
        endpoints=endpoints,
        graph=graph,
        fs=fs,
        spec_hashes=spec_hashes,
        backend=backend,
    )

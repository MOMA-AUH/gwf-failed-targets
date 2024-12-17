import json
import re
import subprocess
from dataclasses import dataclass, fields
from datetime import datetime
from enum import auto, IntEnum
from gwf.core import Context, Target
from gwf_utilization.accounting import _parse_memory_string
from gwf_utilization.main import pretty_size
from json import JSONDecodeError
from pathlib import Path
from texttable import Texttable
from typing import Dict, Generator, List, Optional

from gwf_failed_targets.utilities import tail


TIMEOUT_PATTERN = r"slurmstepd: error: \*\*\* JOB [0-9]+ ON [a-zA-Z0-9_-]+ CANCELLED AT [0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2} DUE TO TIME LIMIT \*\*\*"
OOM_PATTERN = r"slurmstepd: error: Detected [0-9]+ oom_kill event in StepId=[0-9]+.batch. Some of the step tasks have been OOM Killed."


class FailureTypes(IntEnum):
    Unknown = auto()
    Timeout = auto()
    OutOfMemory = auto()
    Submission = auto()
    FileSystem = auto()


@dataclass
class TargetRecord:
    time_of_failure: datetime
    group: str
    node: str
    failure_type: FailureTypes
    exit_code: str
    allocated_memory: int
    used_memory: int
    allocated_walltime: str
    used_walltime: str

    def format_record(self) -> List[str]:
        return [
            self.time_of_failure.isoformat(),
            self.group,
            self.node,
            self.failure_type._name_,
            self.exit_code,
            pretty_size(self.allocated_memory),
            pretty_size(self.used_memory),
            self.allocated_walltime,
            self.used_walltime,
        ]

    @classmethod
    def format_header(self) -> List[str]:
        return [field.name.title().replace("_", "") for field in fields(TargetRecord)]


class SlurmAccounting:
    DEFAULT_FIELDS = [
        "JobID",
        "NodeList",
        "NNodes",
        "NCPUS",
        "ReqMem",
        "MaxRSS",
        "Timelimit",
        "Elapsed",
        "State",
        "ExitCode",
    ]

    def __init__(
        self,
        context: Context,
        targets: List[Target],
        sacct_fields: List[str] = DEFAULT_FIELDS,
    ) -> None:
        self.context = context
        self.targets = targets
        self.sacct_fields = sacct_fields

    @property
    def tracked_jobs(self) -> Dict[str, str]:
        """Load jobs tracked by the slurm backend."""
        tracked_jobs_path = (
            Path(self.context.working_dir) / ".gwf" / "slurm-backend-tracked.json"
        )
        try:
            return json.loads(tracked_jobs_path.read_text())
        except (FileNotFoundError, JSONDecodeError):
            return {}

    def _get_log_modification_time(
        self,
        target: Target,
    ) -> str:
        """Get the last modification time of a target's stderr log file."""
        log_path = Path(self.context.logs_dir) / f"{target.name}.stderr"
        stat = log_path.stat()
        mod_time = datetime.fromtimestamp(stat.st_ctime)
        return mod_time

    def _determine_cause_of_failure(
        self,
        target: Target,
        state: Optional[str] = None,
    ) -> FailureTypes:
        """Determine the cause of failure of a target's slurm job."""
        log_path = Path(self.context.logs_dir) / f"{target.name}.stderr"
        with log_path.open() as f:
            log = "\n".join(tail(f, n=3))

        if state == "TIMEOUT" or re.search(pattern=TIMEOUT_PATTERN, string=log):
            return FailureTypes.Timeout
        elif re.search(pattern=OOM_PATTERN, string=log):
            return FailureTypes.OutOfMemory
        elif "sbatch: error: Batch job submission failed" in log:
            return FailureTypes.Submission
        elif "Device or resource busy" in log:
            return FailureTypes.FileSystem
        return FailureTypes.Unknown

    def fetch(self) -> Generator[TargetRecord, None, None]:
        """Fetch records of failed targets present in tracked jobs."""
        jobs = {
            self.tracked_jobs[target.name]: target
            for target in self.targets
            if target.name in self.tracked_jobs
        }

        if not jobs:
            return

        p = subprocess.run(
            args=f"""sacct \
                --jobs {','.join(jobs.keys())} \
                --format='{','.join(self.sacct_fields)}' \
                --parsable2""",
            shell=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=True,
            text=True,
        )
        header, *records = p.stdout.strip().split("\n")
        header = header.strip().split("|")

        for accounting in records:
            accounting = dict(zip(header, accounting.strip().split("|")))

            if (id := accounting["JobID"]) not in jobs:
                continue

            target = jobs[id]

            yield TargetRecord(
                time_of_failure=self._get_log_modification_time(target=target),
                group=target.name,  # TODO: Add group attribute upon next GWF release
                node=accounting["NodeList"],
                failure_type=self._determine_cause_of_failure(
                    target=target,
                    state=accounting["State"],
                ),
                exit_code=accounting["ExitCode"],
                allocated_memory=_parse_memory_string(
                    memory_string=accounting["ReqMem"],
                    cores=accounting["NCPUS"],
                    nodes=accounting["NNodes"],
                ),
                used_memory=_parse_memory_string(
                    memory_string=accounting["MaxRSS"],
                    cores=accounting["NCPUS"],
                    nodes=accounting["NNodes"],
                ),
                allocated_walltime=accounting["Timelimit"],
                used_walltime=accounting["Elapsed"],
            )

    def to_file(self, path: Path) -> None:
        """Log records of failed targets to a file."""
        if not path.exists():
            path.write_text("\t".join(TargetRecord.format_header()) + "\n")
        for record in self.fetch():
            with path.open(mode="a") as f:
                f.write("\t".join(record.format_record()) + "\n")

    def to_stdout(self) -> None:
        """Print records of failed targets as a table.
        Based on the gwf-utilization implementation."""
        rows = [TargetRecord.format_header()] + [
            record.format_record() for record in self.fetch()
        ]

        table = Texttable()

        table.set_deco(Texttable.BORDER | Texttable.HEADER | Texttable.VLINES)

        ncols = len(rows[0])

        table.set_max_width(0)
        table.set_header_align("l" * ncols)
        table.set_cols_align(["l"] + (ncols - 1) * ["r"])
        table.set_cols_dtype(["t"] * ncols)

        table.add_rows(rows)

        print(table.draw())

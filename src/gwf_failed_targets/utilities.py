from io import TextIOWrapper
from typing import List


def tail(f: TextIOWrapper, n: int = 10) -> List[str]:
    """Get the last n lines of a file.
    Source: https://stackoverflow.com/a/280083"""
    assert n >= 0
    pos, lines = n + 1, []
    while len(lines) <= n:
        try:
            f.seek(-pos, 2)
        except IOError:
            f.seek(0)
            break
        finally:
            lines = list(f)
        pos *= 2
    return lines[-n:]

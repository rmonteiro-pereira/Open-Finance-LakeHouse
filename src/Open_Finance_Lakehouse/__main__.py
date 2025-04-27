"""Open-Finance-Lakehouse file for ensuring the package is executable
as `open-finance-lakehouse` and `python -m open_finance_lakehouse`
"""
import sys
from pathlib import Path
from typing import Any

from kedro.framework.cli.utils import find_run_command
from kedro.framework.project import configure_project
import warnings
warnings.filterwarnings("ignore", category=DeprecationWarning)
warnings.filterwarnings("ignore", category=UserWarning)
try:
    from kedro.framework.hooks import KedroDeprecationWarning
    warnings.filterwarnings("ignore", category=KedroDeprecationWarning)
except ImportError:
    pass


def main(*args, **kwargs) -> Any:
    package_name = Path(__file__).parent.name
    configure_project(package_name)

    interactive = hasattr(sys, 'ps1')
    kwargs["standalone_mode"] = not interactive

    run = find_run_command(package_name)
    return run(*args, **kwargs)


if __name__ == "__main__":
    main()

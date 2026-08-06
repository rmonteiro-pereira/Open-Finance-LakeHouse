"""The published release — the OFL's primary interface.

The lakehouse is the factory; this package is the counter. Everything a consumer can
reach without a credential comes through here: parquet tables, a manifest that states
the grain and the freshness inputs, schema contracts with a compatibility policy, and
the gates that decide whether any of it may be published at all.

Nothing in here reads MinIO. That is the whole point — a surface that needs the cluster
is a surface that dies with the cluster.
"""

from ofl.release.build import ReleaseError, build_release, read_source
from ofl.release.contracts import TableContract, load_contracts
from ofl.release.verify import UsageError, VerifyReport, verify_release

__all__ = [
    "ReleaseError",
    "TableContract",
    "UsageError",
    "VerifyReport",
    "build_release",
    "load_contracts",
    "read_source",
    "verify_release",
]

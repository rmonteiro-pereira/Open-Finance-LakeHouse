"""
LakeFS Operators for Airflow.

Custom operators for managing LakeFS branches, commits, and merges.
"""

import logging
from typing import Optional

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

logger = logging.getLogger(__name__)

try:
    import sys
    from pathlib import Path

    # Add project root to path
    project_root = Path(__file__).parent.parent.parent.parent
    sys.path.insert(0, str(project_root))

    from src.open_finance_lakehouse.utils.lakefs_client import LakeFSClient
    from src.open_finance_lakehouse.utils.lakefs_operations import LakeFSOperations
except ImportError as e:
    logger.warning(f"Failed to import LakeFS utilities: {e}")


class LakeFSCreateBranchOperator(BaseOperator):
    """Operator to create or reuse a LakeFS branch."""

    @apply_defaults
    def __init__(
        self,
        branch_name: str = "dev",
        source_branch: str = "main",
        lakefs_endpoint: Optional[str] = None,
        lakefs_access_key: Optional[str] = None,
        lakefs_secret_key: Optional[str] = None,
        repository: str = "open-finance-lakehouse",
        *args,
        **kwargs,
    ):
        """
        Initialize operator.

        Args:
            branch_name: Name of branch to create/reuse
            source_branch: Source branch to branch from
            lakefs_endpoint: LakeFS endpoint URL
            lakefs_access_key: LakeFS access key
            lakefs_secret_key: LakeFS secret key
            repository: Repository name
        """
        super().__init__(*args, **kwargs)
        self.branch_name = branch_name
        self.source_branch = source_branch
        self.lakefs_endpoint = lakefs_endpoint
        self.lakefs_access_key = lakefs_access_key
        self.lakefs_secret_key = lakefs_secret_key
        self.repository = repository

    def execute(self, context):
        """Execute operator."""
        import os

        endpoint = self.lakefs_endpoint or os.getenv("LAKEFS_ENDPOINT")
        access_key = self.lakefs_access_key or os.getenv("LAKEFS_ACCESS_KEY")
        secret_key = self.lakefs_secret_key or os.getenv("LAKEFS_SECRET_KEY")

        if not all([endpoint, access_key, secret_key]):
            raise ValueError("LakeFS credentials not configured")

        client = LakeFSClient(
            endpoint=endpoint,
            access_key=access_key,
            secret_key=secret_key,
            repository=self.repository,
        )
        operations = LakeFSOperations(client)

        branch_info = operations.prepare_dev_branch(
            branch_name=self.branch_name, source_branch=self.source_branch
        )
        logger.info(f"Prepared branch: {self.branch_name}")
        return branch_info


class LakeFSCommitOperator(BaseOperator):
    """Operator to commit changes to LakeFS."""

    @apply_defaults
    def __init__(
        self,
        branch_name: str = "dev",
        layer: str = "raw",
        pipeline_name: Optional[str] = None,
        lakefs_endpoint: Optional[str] = None,
        lakefs_access_key: Optional[str] = None,
        lakefs_secret_key: Optional[str] = None,
        repository: str = "open-finance-lakehouse",
        *args,
        **kwargs,
    ):
        """
        Initialize operator.

        Args:
            branch_name: Branch name
            layer: Data layer (raw, bronze, silver, gold)
            pipeline_name: Pipeline name
            lakefs_endpoint: LakeFS endpoint URL
            lakefs_access_key: LakeFS access key
            lakefs_secret_key: LakeFS secret key
            repository: Repository name
        """
        super().__init__(*args, **kwargs)
        self.branch_name = branch_name
        self.layer = layer
        self.pipeline_name = pipeline_name
        self.lakefs_endpoint = lakefs_endpoint
        self.lakefs_access_key = lakefs_access_key
        self.lakefs_secret_key = lakefs_secret_key
        self.repository = repository

    def execute(self, context):
        """Execute operator."""
        import os

        endpoint = self.lakefs_endpoint or os.getenv("LAKEFS_ENDPOINT")
        access_key = self.lakefs_access_key or os.getenv("LAKEFS_ACCESS_KEY")
        secret_key = self.lakefs_secret_key or os.getenv("LAKEFS_SECRET_KEY")

        if not all([endpoint, access_key, secret_key]):
            raise ValueError("LakeFS credentials not configured")

        client = LakeFSClient(
            endpoint=endpoint,
            access_key=access_key,
            secret_key=secret_key,
            repository=self.repository,
        )
        operations = LakeFSOperations(client)

        pipeline_name = self.pipeline_name or context.get("dag").dag_id
        commit_result = operations.commit_layer(
            branch_name=self.branch_name,
            layer=self.layer,
            pipeline_name=pipeline_name,
        )
        logger.info(f"Committed {self.layer} layer to branch {self.branch_name}")
        return commit_result


class LakeFSMergeOperator(BaseOperator):
    """Operator to merge dev branch to main in LakeFS."""

    @apply_defaults
    def __init__(
        self,
        source_branch: str = "dev",
        destination_branch: str = "main",
        validation_passed: bool = True,
        lakefs_endpoint: Optional[str] = None,
        lakefs_access_key: Optional[str] = None,
        lakefs_secret_key: Optional[str] = None,
        repository: str = "open-finance-lakehouse",
        *args,
        **kwargs,
    ):
        """
        Initialize operator.

        Args:
            source_branch: Source branch to merge from
            destination_branch: Destination branch to merge into
            validation_passed: Whether validation passed
            lakefs_endpoint: LakeFS endpoint URL
            lakefs_access_key: LakeFS access key
            lakefs_secret_key: LakeFS secret key
            repository: Repository name
        """
        super().__init__(*args, **kwargs)
        self.source_branch = source_branch
        self.destination_branch = destination_branch
        self.validation_passed = validation_passed
        self.lakefs_endpoint = lakefs_endpoint
        self.lakefs_access_key = lakefs_access_key
        self.lakefs_secret_key = lakefs_secret_key
        self.repository = repository

    def execute(self, context):
        """Execute operator."""
        import os

        endpoint = self.lakefs_endpoint or os.getenv("LAKEFS_ENDPOINT")
        access_key = self.lakefs_access_key or os.getenv("LAKEFS_ACCESS_KEY")
        secret_key = self.lakefs_secret_key or os.getenv("LAKEFS_SECRET_KEY")

        if not all([endpoint, access_key, secret_key]):
            raise ValueError("LakeFS credentials not configured")

        client = LakeFSClient(
            endpoint=endpoint,
            access_key=access_key,
            secret_key=secret_key,
            repository=self.repository,
        )
        operations = LakeFSOperations(client)

        # Get validation_passed from context if it's a template
        validation_passed = self.validation_passed
        if isinstance(validation_passed, str) and validation_passed.startswith("{{"):
            # It's a template, try to get from XCom
            try:
                validation_passed = context["ti"].xcom_pull(
                    task_ids="check_validation_result"
                )
            except Exception:
                validation_passed = True  # Default to True if can't get from XCom

        merge_result = operations.merge_to_main(
            source_branch=self.source_branch,
            destination_branch=self.destination_branch,
            validation_passed=validation_passed,
        )

        if merge_result:
            logger.info(
                f"Merged {self.source_branch} to {self.destination_branch}"
            )
        else:
            logger.warning(
                f"Merge skipped: validation_passed={self.validation_passed}"
            )

        return merge_result


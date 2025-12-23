"""
Great Expectations Operators for Airflow.

Custom operators for running data quality validations.
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

    from src.open_finance_lakehouse.utils.gx_validator import GXValidator
except ImportError as e:
    logger.warning(f"Failed to import Great Expectations utilities: {e}")


class GreatExpectationsOperator(BaseOperator):
    """Operator to run Great Expectations validations."""

    @apply_defaults
    def __init__(
        self,
        asset_name: str,
        suite_name: str,
        dataframe_task_id: Optional[str] = None,
        context_root_dir: Optional[str] = None,
        *args,
        **kwargs,
    ):
        """
        Initialize operator.

        Args:
            asset_name: Name of the GX asset to validate
            suite_name: Name of the expectation suite
            dataframe_task_id: Task ID that produces the DataFrame (optional)
            context_root_dir: GX context root directory
        """
        super().__init__(*args, **kwargs)
        self.asset_name = asset_name
        self.suite_name = suite_name
        self.dataframe_task_id = dataframe_task_id
        self.context_root_dir = context_root_dir

    def execute(self, context):
        """Execute operator."""
        validator = GXValidator(context_root_dir=self.context_root_dir)

        # If dataframe_task_id is provided, register the DataFrame first
        if self.dataframe_task_id:
            dataframe = context["ti"].xcom_pull(task_ids=self.dataframe_task_id)
            if dataframe is not None:
                validator.register_dataframe_asset(
                    asset_name=self.asset_name, dataframe=dataframe, suite_name=self.suite_name
                )

        # Run validation
        validation_result = validator.run_validation(
            asset_name=self.asset_name, suite_name=self.suite_name
        )

        # Get summary
        summary = validator.get_validation_summary(validation_result)

        # Store result in XCom
        context["ti"].xcom_push(key="validation_result", value=validation_result)
        context["ti"].xcom_push(key="validation_summary", value=summary)

        if not validation_result["success"]:
            logger.error(f"Validation failed for {self.asset_name}")
            logger.error(f"Failed expectations: {summary['failed_expectations']}")
            raise ValueError(f"Validation failed: {summary['failed_expectations']}")

        logger.info(f"Validation passed for {self.asset_name}")
        return validation_result


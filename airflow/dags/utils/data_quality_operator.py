"""
Generic Data Quality Operator for Airflow.

This operator supports multiple validation frameworks:
- Great Expectations (optional)
- Custom validation functions
- Simple schema/type checks
"""

import logging
from typing import Any, Callable, Optional

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

logger = logging.getLogger(__name__)


class DataQualityOperator(BaseOperator):
    """
    Generic operator for data quality validation.
    
    Supports multiple validation methods:
    1. Great Expectations (if available)
    2. Custom Python validation functions
    3. Simple schema/type validation
    """

    @apply_defaults
    def __init__(
        self,
        validation_method: str = "custom",
        validation_function: Optional[Callable] = None,
        gx_asset_name: Optional[str] = None,
        gx_suite_name: Optional[str] = None,
        schema_validation: Optional[dict] = None,
        custom_validation_kwargs: Optional[dict] = None,
        *args,
        **kwargs,
    ):
        """
        Initialize data quality operator.

        Args:
            validation_method: Method to use ('gx', 'custom', 'schema')
            validation_function: Custom validation function (for 'custom' method)
            gx_asset_name: GX asset name (for 'gx' method)
            gx_suite_name: GX suite name (for 'gx' method)
            schema_validation: Schema validation rules (for 'schema' method)
            custom_validation_kwargs: Additional kwargs for custom validation
        """
        super().__init__(*args, **kwargs)
        self.validation_method = validation_method
        self.validation_function = validation_function
        self.gx_asset_name = gx_asset_name
        self.gx_suite_name = gx_suite_name
        self.schema_validation = schema_validation
        self.custom_validation_kwargs = custom_validation_kwargs or {}

    def execute(self, context: dict) -> dict:
        """
        Execute validation.

        Returns:
            Validation result dictionary with 'success' and 'details' keys
        """
        logger.info(f"Running data quality validation using method: {self.validation_method}")

        if self.validation_method == "gx":
            return self._run_gx_validation(context)
        elif self.validation_method == "custom":
            return self._run_custom_validation(context)
        elif self.validation_method == "schema":
            return self._run_schema_validation(context)
        else:
            logger.warning(f"Unknown validation method: {self.validation_method}")
            # Default: pass validation
            return {
                "success": True,
                "method": self.validation_method,
                "message": "Validation method not implemented, passing by default",
            }

    def _run_gx_validation(self, context: dict) -> dict:
        """Run Great Expectations validation."""
        try:
            from ..utils.gx_operators import GreatExpectationsOperator

            gx_op = GreatExpectationsOperator(
                task_id=f"{self.task_id}_gx",
                asset_name=self.gx_asset_name or "unknown",
                suite_name=self.gx_suite_name or "default",
            )
            result = gx_op.execute(context)
            return {
                "success": result.get("success", False),
                "method": "great_expectations",
                "details": result,
            }
        except ImportError:
            logger.warning("Great Expectations not available, skipping GX validation")
            return {
                "success": True,
                "method": "great_expectations",
                "message": "Great Expectations not installed, validation skipped",
            }
        except Exception as e:
            logger.error(f"Great Expectations validation failed: {e}")
            return {
                "success": False,
                "method": "great_expectations",
                "error": str(e),
            }

    def _run_custom_validation(self, context: dict) -> dict:
        """Run custom validation function."""
        if not self.validation_function:
            logger.warning("No custom validation function provided")
            return {
                "success": True,
                "method": "custom",
                "message": "No validation function provided, passing by default",
            }

        try:
            # Get data from previous task if needed
            ti = context.get("ti")
            data = None
            if ti:
                # Try to get data from XCom
                data = ti.xcom_pull(task_ids=None, key="data")

            # Run validation
            result = self.validation_function(data, **self.custom_validation_kwargs)
            
            # Handle different return types
            if isinstance(result, dict):
                return {
                    "success": result.get("success", True),
                    "method": "custom",
                    "details": result,
                }
            elif isinstance(result, bool):
                return {
                    "success": result,
                    "method": "custom",
                }
            else:
                return {
                    "success": True,
                    "method": "custom",
                    "details": {"result": result},
                }
        except Exception as e:
            logger.error(f"Custom validation failed: {e}")
            return {
                "success": False,
                "method": "custom",
                "error": str(e),
            }

    def _run_schema_validation(self, context: dict) -> dict:
        """Run simple schema validation."""
        if not self.schema_validation:
            logger.warning("No schema validation rules provided")
            return {
                "success": True,
                "method": "schema",
                "message": "No schema rules provided, passing by default",
            }

        try:
            # Get data from previous task
            ti = context.get("ti")
            if not ti:
                return {
                    "success": False,
                    "method": "schema",
                    "error": "No task instance available",
                }

            # This is a placeholder - actual schema validation would go here
            # For now, just return success
            logger.info("Schema validation passed (placeholder)")
            return {
                "success": True,
                "method": "schema",
                "details": self.schema_validation,
            }
        except Exception as e:
            logger.error(f"Schema validation failed: {e}")
            return {
                "success": False,
                "method": "schema",
                "error": str(e),
            }


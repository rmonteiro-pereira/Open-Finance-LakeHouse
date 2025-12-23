"""
OpenMetadata Operators for Airflow.

Custom operators for registering datasets and lineage in OpenMetadata.
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

    from src.open_finance_lakehouse.utils.metadata_registry import MetadataRegistry
    from src.open_finance_lakehouse.utils.openmetadata_client import OpenMetadataClient
except ImportError as e:
    logger.warning(f"Failed to import OpenMetadata utilities: {e}")


class OpenMetadataRegisterOperator(BaseOperator):
    """Operator to register datasets in OpenMetadata."""

    @apply_defaults
    def __init__(
        self,
        layer: str,
        dataset_name: str,
        storage_path: str,
        source: str,
        pipeline_name: Optional[str] = None,
        format_type: Optional[str] = None,
        schema: Optional[dict] = None,
        openmetadata_endpoint: Optional[str] = None,
        openmetadata_auth_token: Optional[str] = None,
        openmetadata_username: Optional[str] = None,
        openmetadata_password: Optional[str] = None,
        *args,
        **kwargs,
    ):
        """
        Initialize operator.

        Args:
            layer: Data layer (raw, bronze, silver, gold)
            dataset_name: Dataset name
            storage_path: S3 path to dataset
            source: Data source (BACEN, IBGE, etc.)
            pipeline_name: Pipeline name
            format_type: Format type (for Raw layer)
            schema: Schema definition (for Bronze+ layers)
            openmetadata_endpoint: OpenMetadata endpoint
            openmetadata_auth_token: Auth token
            openmetadata_username: Username
            openmetadata_password: Password
        """
        super().__init__(*args, **kwargs)
        self.layer = layer
        self.dataset_name = dataset_name
        self.storage_path = storage_path
        self.source = source
        self.pipeline_name = pipeline_name
        self.format_type = format_type
        self.schema = schema
        self.openmetadata_endpoint = openmetadata_endpoint
        self.openmetadata_auth_token = openmetadata_auth_token
        self.openmetadata_username = openmetadata_username
        self.openmetadata_password = openmetadata_password

    def execute(self, context):
        """Execute operator."""
        import os

        endpoint = self.openmetadata_endpoint or os.getenv(
            "OPENMETADATA_ENDPOINT", "http://localhost:8585"
        )
        auth_token = self.openmetadata_auth_token or os.getenv(
            "OPENMETADATA_AUTH_TOKEN"
        )
        username = self.openmetadata_username or os.getenv("OPENMETADATA_USERNAME")
        password = self.openmetadata_password or os.getenv("OPENMETADATA_PASSWORD")

        client = OpenMetadataClient(
            endpoint=endpoint,
            auth_token=auth_token,
            username=username,
            password=password,
        )
        registry = MetadataRegistry(client)

        pipeline_name = self.pipeline_name or context.get("dag").dag_id

        if self.layer == "raw":
            # Register Raw dataset
            entity = registry.register_raw_dataset(
                dataset_name=self.dataset_name,
                storage_path=self.storage_path,
                format_type=self.format_type or "JSON",
                source=self.source,
                pipeline_name=pipeline_name,
            )
        else:
            # For other layers, schema should be provided
            if not self.schema:
                raise ValueError(f"Schema required for {self.layer} layer")

            entity = registry.register_dataset_with_schema(
                layer=self.layer,
                dataset_name=self.dataset_name,
                storage_path=self.storage_path,
                schema=self.schema,
                source=self.source,
                pipeline_name=pipeline_name,
            )

        logger.info(
            f"Registered {self.layer} dataset in OpenMetadata: {self.dataset_name}"
        )
        return entity


class OpenMetadataLineageOperator(BaseOperator):
    """Operator to register lineage in OpenMetadata."""

    @apply_defaults
    def __init__(
        self,
        pipeline_name: str,
        layers: list[str],
        dataset_names: list[str],
        source: str,
        openmetadata_endpoint: Optional[str] = None,
        openmetadata_auth_token: Optional[str] = None,
        openmetadata_username: Optional[str] = None,
        openmetadata_password: Optional[str] = None,
        *args,
        **kwargs,
    ):
        """
        Initialize operator.

        Args:
            pipeline_name: Pipeline name
            layers: List of layers (raw, bronze, silver, gold)
            dataset_names: List of dataset names for each layer
            source: Data source
            openmetadata_endpoint: OpenMetadata endpoint
            openmetadata_auth_token: Auth token
            openmetadata_username: Username
            openmetadata_password: Password
        """
        super().__init__(*args, **kwargs)
        self.pipeline_name = pipeline_name
        self.layers = layers
        self.dataset_names = dataset_names
        self.source = source
        self.openmetadata_endpoint = openmetadata_endpoint
        self.openmetadata_auth_token = openmetadata_auth_token
        self.openmetadata_username = openmetadata_username
        self.openmetadata_password = openmetadata_password

    def execute(self, context):
        """Execute operator."""
        import os

        endpoint = self.openmetadata_endpoint or os.getenv(
            "OPENMETADATA_ENDPOINT", "http://localhost:8585"
        )
        auth_token = self.openmetadata_auth_token or os.getenv(
            "OPENMETADATA_AUTH_TOKEN"
        )
        username = self.openmetadata_username or os.getenv("OPENMETADATA_USERNAME")
        password = self.openmetadata_password or os.getenv("OPENMETADATA_PASSWORD")

        client = OpenMetadataClient(
            endpoint=endpoint,
            auth_token=auth_token,
            username=username,
            password=password,
        )
        registry = MetadataRegistry(client)

        lineage = registry.register_lineage_chain(
            source=self.source,
            pipeline_name=self.pipeline_name,
            layers=self.layers,
            dataset_names=self.dataset_names,
        )

        logger.info(f"Registered lineage for {self.pipeline_name}")
        return lineage


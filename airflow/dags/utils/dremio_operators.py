"""
Dremio Operators for Airflow.

Custom operators for managing Dremio views.
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

    from src.open_finance_lakehouse.utils.dremio_client import DremioClient
    from src.open_finance_lakehouse.utils.dremio_views import DremioViews
except ImportError as e:
    logger.warning(f"Failed to import Dremio utilities: {e}")


class DremioRefreshViewOperator(BaseOperator):
    """Operator to refresh Dremio views."""

    @apply_defaults
    def __init__(
        self,
        view_fqn: Optional[str] = None,
        view_path: Optional[list[str]] = None,
        dremio_endpoint: Optional[str] = None,
        dremio_username: Optional[str] = None,
        dremio_password: Optional[str] = None,
        *args,
        **kwargs,
    ):
        """
        Initialize operator.

        Args:
            view_fqn: Fully qualified view name (e.g., lakehouse.gold.selic_kpis)
            view_path: Path to refresh all views from (e.g., ['lakehouse', 'gold'])
            dremio_endpoint: Dremio endpoint URL
            dremio_username: Dremio username
            dremio_password: Dremio password
        """
        super().__init__(*args, **kwargs)
        self.view_fqn = view_fqn
        self.view_path = view_path or ["lakehouse", "gold"]
        self.dremio_endpoint = dremio_endpoint
        self.dremio_username = dremio_username
        self.dremio_password = dremio_password

    def execute(self, context):
        """Execute operator."""
        import os

        endpoint = self.dremio_endpoint or os.getenv("DREMIO_ENDPOINT")
        username = self.dremio_username or os.getenv("DREMIO_USERNAME")
        password = self.dremio_password or os.getenv("DREMIO_PASSWORD")

        if not all([endpoint, username, password]):
            raise ValueError("Dremio credentials not configured")

        client = DremioClient(
            endpoint=endpoint, username=username, password=password
        )
        views_manager = DremioViews(client)

        if self.view_fqn:
            # Refresh specific view
            refreshed = client.refresh_view(self.view_fqn)
            logger.info(f"Refreshed view: {self.view_fqn}")
            return refreshed
        else:
            # Refresh all views in path
            refreshed = views_manager.refresh_all_views(self.view_path)
            logger.info(f"Refreshed {len(refreshed)} views in {'.'.join(self.view_path)}")
            return refreshed


class DremioCreateViewOperator(BaseOperator):
    """Operator to create a Dremio view."""

    @apply_defaults
    def __init__(
        self,
        view_name: str,
        source_path: str,
        view_path: Optional[list[str]] = None,
        dremio_endpoint: Optional[str] = None,
        dremio_username: Optional[str] = None,
        dremio_password: Optional[str] = None,
        *args,
        **kwargs,
    ):
        """
        Initialize operator.

        Args:
            view_name: Name of the view
            source_path: S3 path to source data
            view_path: Path where view should be created
            dremio_endpoint: Dremio endpoint URL
            dremio_username: Dremio username
            dremio_password: Dremio password
        """
        super().__init__(*args, **kwargs)
        self.view_name = view_name
        self.source_path = source_path
        self.view_path = view_path or ["lakehouse", "gold"]
        self.dremio_endpoint = dremio_endpoint
        self.dremio_username = dremio_username
        self.dremio_password = dremio_password

    def execute(self, context):
        """Execute operator."""
        import os

        endpoint = self.dremio_endpoint or os.getenv("DREMIO_ENDPOINT")
        username = self.dremio_username or os.getenv("DREMIO_USERNAME")
        password = self.dremio_password or os.getenv("DREMIO_PASSWORD")

        if not all([endpoint, username, password]):
            raise ValueError("Dremio credentials not configured")

        client = DremioClient(
            endpoint=endpoint, username=username, password=password
        )
        views_manager = DremioViews(client)

        view = views_manager.create_gold_view(
            view_name=self.view_name,
            source_path=self.source_path,
            view_path=self.view_path,
        )

        logger.info(f"Created view: {self.view_name}")
        return view


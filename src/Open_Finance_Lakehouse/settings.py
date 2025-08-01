"""Project settings. There is no need to edit this file unless you want to change values
from the Kedro defaults. For further information, including these default values, see
https://docs.kedro.org/en/stable/kedro_project_setup/settings.html."""

import os
import locale

from dotenv import load_dotenv
from kedro.config import OmegaConfigLoader

# Instantiated project hooks.
# Commented out MlflowHook to prevent parameter conflicts
# from kedro_mlflow.framework.hooks import MlflowHook

from .hooks import SparkHooks, UTF8EncodingHook  # Import from hooks package

# Load environment variables from .env file
load_dotenv()

# Fix for Windows UTF-8 encoding issues with MLflow
# This ensures Brazilian Portuguese characters are handled correctly
os.environ["PYTHONUTF8"] = "1"
os.environ["PYTHONIOENCODING"] = "utf-8"

# Set locale for proper encoding handling
try:
    locale.setlocale(locale.LC_ALL, 'pt_BR.UTF-8')
except locale.Error:
    try:
        locale.setlocale(locale.LC_ALL, 'Portuguese_Brazil.1252')
    except locale.Error:
        pass  # Fall back to default locale

# Hooks are executed in a Last-In-First-Out (LIFO) order.
# UTF8EncodingHook ensures proper encoding for Brazilian Portuguese characters
HOOKS = (UTF8EncodingHook(), SparkHooks())

# Class that manages how configuration is loaded.
CONFIG_LOADER_CLASS = OmegaConfigLoader
# Keyword arguments to pass to the `CONFIG_LOADER_CLASS` constructor.
CONFIG_LOADER_ARGS = {
    "base_env": "base",
    "default_run_env": "local",
    "config_patterns": {
        "spark": ["spark*", "spark*/**"],
    },
    "custom_resolvers": {
        "env": lambda var: os.getenv(var),
    }
}

# Class that manages Kedro's library components.
# from kedro.framework.context import KedroContext
# CONTEXT_CLASS = KedroContext

# Class that manages the Data Catalog.
# from kedro.io import DataCatalog
# DATA_CATALOG_CLASS = DataCatalog

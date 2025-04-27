"""Open-Finance-Lakehouse
"""
import warnings

warnings.filterwarnings("ignore", category=DeprecationWarning)
warnings.filterwarnings("ignore", category=UserWarning)
try:
    from kedro.framework.hooks import KedroDeprecationWarning
    warnings.filterwarnings("ignore", category=KedroDeprecationWarning)
except ImportError:
    pass

__version__ = "0.1"

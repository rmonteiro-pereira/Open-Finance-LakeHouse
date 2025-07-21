"""
Temporary hook configuration to disable MLflow for testing B3 pipeline
"""
from kedro.framework.hooks import hook_impl
from kedro.pipeline import Pipeline

class DisableMLflowHook:
    """Hook to disable MLflow logging temporarily"""
    
    @hook_impl
    def before_node_run(self, node, catalog, inputs, is_async, session_id):
        """Disable MLflow parameter logging"""
        return {}
    
    @hook_impl  
    def after_node_run(self, node, catalog, inputs, outputs, is_async, session_id):
        """Disable MLflow metric logging"""
        return {}

"""
Pipeline execution tracker and summary reporter
"""
import time
import logging
from typing import Any

logger = logging.getLogger(__name__)

class PipelineTracker:
    def __init__(self):
        self.start_time = time.time()
        self.stage_times = {}
        self.current_stage = None
        
    def start_stage(self, stage_name: str):
        """Start timing a pipeline stage"""
        if self.current_stage:
            self.end_stage()
        self.current_stage = stage_name
        self.stage_times[stage_name] = {"start": time.time()}
        logger.info(f"🚀 Starting pipeline stage: {stage_name}")
        
    def end_stage(self):
        """End timing the current stage"""
        if self.current_stage and self.current_stage in self.stage_times:
            end_time = time.time()
            start_time = self.stage_times[self.current_stage]["start"]
            duration = end_time - start_time
            self.stage_times[self.current_stage]["duration"] = duration
            logger.info(f"✅ Completed {self.current_stage} in {duration:.2f}s")
            self.current_stage = None
            
    def get_summary(self) -> dict[str, Any]:
        """Get pipeline execution summary"""
        if self.current_stage:
            self.end_stage()
            
        total_time = time.time() - self.start_time
        
        summary = {
            "total_execution_time": total_time,
            "stage_timings": {},
            "fastest_stage": None,
            "slowest_stage": None
        }
        
        durations = {}
        for stage, timing in self.stage_times.items():
            if "duration" in timing:
                duration = timing["duration"]
                summary["stage_timings"][stage] = duration
                durations[stage] = duration
                
        if durations:
            summary["fastest_stage"] = min(durations, key=durations.get)
            summary["slowest_stage"] = max(durations, key=durations.get)
            
        return summary
        
    def print_summary(self):
        """Print a formatted pipeline summary"""
        summary = self.get_summary()
        
        logger.info("=" * 60)
        logger.info("📊 PIPELINE EXECUTION SUMMARY")
        logger.info("=" * 60)
        logger.info(f"⏱️  Total execution time: {summary['total_execution_time']:.2f}s")
        logger.info("")
        logger.info("🏃 Stage timings:")
        
        for stage, duration in summary["stage_timings"].items():
            percentage = (duration / summary["total_execution_time"]) * 100
            logger.info(f"   {stage}: {duration:.2f}s ({percentage:.1f}%)")
            
        if summary["fastest_stage"] and summary["slowest_stage"]:
            logger.info("")
            logger.info(f"🚀 Fastest stage: {summary['fastest_stage']} ({summary['stage_timings'][summary['fastest_stage']]:.2f}s)")
            logger.info(f"🐌 Slowest stage: {summary['slowest_stage']} ({summary['stage_timings'][summary['slowest_stage']]:.2f}s)")
            
        logger.info("=" * 60)

# Global tracker instance
pipeline_tracker = PipelineTracker()

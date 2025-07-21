"""
Data Quality Validation Module
Provides utilities for data quality validation and reporting
"""

from typing import NamedTuple, Any
from datetime import datetime


class ValidationResult(NamedTuple):
    """Represents the result of a data validation rule"""
    rule_name: str
    passed: bool
    message: str
    severity: str  # 'info', 'warning', 'error'


def generate_quality_report(validation_results: list[ValidationResult], dataset_name: str) -> dict[str, Any]:
    """
    Generate a comprehensive quality report from validation results
    
    Args:
        validation_results: List of validation results
        dataset_name: Name of the dataset being validated
        
    Returns:
        Quality report dictionary
    """
    passed_count = sum(1 for result in validation_results if result.passed)
    total_count = len(validation_results)
    
    # Categorize by severity
    errors = [r for r in validation_results if r.severity == 'error']
    warnings = [r for r in validation_results if r.severity == 'warning']
    infos = [r for r in validation_results if r.severity == 'info']
    
    # Determine overall status
    if any(not r.passed for r in errors):
        overall_status = 'FAILED'
    elif any(not r.passed for r in warnings):
        overall_status = 'WARNING'
    else:
        overall_status = 'PASSED'
    
    return {
        "dataset_name": dataset_name,
        "validation_timestamp": datetime.now().isoformat(),
        "overall_status": overall_status,
        "summary": {
            "total": total_count,
            "passed": passed_count,
            "failed": total_count - passed_count,
            "pass_rate": passed_count / total_count if total_count > 0 else 1.0
        },
        "by_severity": {
            "errors": {
                "total": len(errors),
                "failed": len([r for r in errors if not r.passed])
            },
            "warnings": {
                "total": len(warnings),
                "failed": len([r for r in warnings if not r.passed])
            },
            "info": {
                "total": len(infos),
                "failed": len([r for r in infos if not r.passed])
            }
        },
        "detailed_results": [
            {
                "rule_name": result.rule_name,
                "status": "PASSED" if result.passed else "FAILED",
                "severity": result.severity,
                "message": result.message
            }
            for result in validation_results
        ]
    }


class DataQualityValidator:
    """Simple data quality validator - placeholder for future expansion"""
    
    def __init__(self):
        self.rules = []
    
    def add_rule(self, rule_name: str, check_function, severity: str = 'error'):
        """Add a validation rule"""
        self.rules.append({
            "name": rule_name,
            "check": check_function, 
            "severity": severity
        })
    
    def validate(self, data) -> list[ValidationResult]:
        """Run all validation rules on data"""
        results = []
        for rule in self.rules:
            try:
                passed = rule["check"](data)
                result = ValidationResult(
                    rule_name=rule["name"],
                    passed=passed,
                    message="Check passed" if passed else "Check failed",
                    severity=rule["severity"]
                )
                results.append(result)
            except Exception as e:
                result = ValidationResult(
                    rule_name=rule["name"],
                    passed=False,
                    message=f"Validation error: {e}",
                    severity="error"
                )
                results.append(result)
        
        return results

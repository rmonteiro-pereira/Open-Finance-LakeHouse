# PowerShell script to run Kedro pipeline with suppressed Spark command output
# Usage: .\run_kedro_quiet.ps1

Write-Host "Running Kedro pipeline with suppressed Spark command output..." -ForegroundColor Green

# Run the Python wrapper script
& uv run python run_kedro_quiet.py

# Check exit code
if ($LASTEXITCODE -eq 0) {
    Write-Host "Kedro pipeline completed successfully!" -ForegroundColor Green
} else {
    Write-Host "Kedro pipeline failed with exit code: $LASTEXITCODE" -ForegroundColor Red
}

exit $LASTEXITCODE

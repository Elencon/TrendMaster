# PowerShell script to clean up locked log files
# This script helps resolve the "file in use" error for log files

Write-Host "ETL Log File Cleanup Utility" -ForegroundColor Green
Write-Host "===============================" -ForegroundColor Green

# Check if any Python processes are running that might have the log files open
$pythonProcesses = Get-Process -Name "python*" -ErrorAction SilentlyContinue
if ($pythonProcesses) {
    Write-Host "`nFound Python processes that might be using log files:" -ForegroundColor Yellow
    $pythonProcesses | Format-Table ProcessName, Id, StartTime -AutoSize
    
    $response = Read-Host "`nDo you want to close these Python processes? (y/N)"
    if ($response -eq 'y' -or $response -eq 'Y') {
        try {
            Stop-Process -Name "python*" -Force
            Write-Host "Python processes closed." -ForegroundColor Green
            Start-Sleep -Seconds 2
        } catch {
            Write-Host "Failed to close Python processes: $_" -ForegroundColor Red
        }
    }
}

# Try to use the Python cache cleaner with log cleanup
Write-Host "`nAttempting to clean log files using Python cache cleaner..." -ForegroundColor Cyan

try {
    # First close logging handlers
    python src/cache_cleaner.py --close-loggers
    Start-Sleep -Seconds 1
    
    # Then clean with logs
    python src/cache_cleaner.py --logs --force-logs
} catch {
    Write-Host "Python cache cleaner failed: $_" -ForegroundColor Red
}

# Manual cleanup as fallback
Write-Host "`nAttempting manual log file cleanup..." -ForegroundColor Cyan

$logFiles = @(
    "logs\etl_api.log",
    "logs\etl_database.log", 
    "logs\etl_main.log",
    "logs\etl_validation.log"
)

foreach ($logFile in $logFiles) {
    if (Test-Path $logFile) {
        try {
            Remove-Item $logFile -Force
            Write-Host "Removed: $logFile" -ForegroundColor Green
        } catch {
            Write-Host "Could not remove: $logFile - $_" -ForegroundColor Yellow
        }
    } else {
        Write-Host "Not found: $logFile" -ForegroundColor Gray
    }
}

Write-Host "`nLog cleanup completed!" -ForegroundColor Green
Write-Host "If files are still locked, try:" -ForegroundColor Cyan
Write-Host "1. Close all Python/ETL applications" -ForegroundColor Cyan
Write-Host "2. Restart your terminal/PowerShell" -ForegroundColor Cyan
Write-Host "3. Run this script again" -ForegroundColor Cyan

Read-Host "`nPress Enter to exit"
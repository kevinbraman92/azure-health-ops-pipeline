@echo off
setlocal ENABLEDELAYEDEXPANSION

REM ==============================================================
REM  ETL Loader
REM  Activates virtual environment and runs etl_load.py
REM  Author: Kevin Braman
REM ==============================================================

REM ===== CONFIGURATION =====
SET "REPO_DIR=%~dp0"
SET "VENV_DIR=%REPO_DIR%scripts\.venv"
SET "PYTHON_EXE=%VENV_DIR%\Scripts\python.exe"
SET "SCRIPT_PATH=%REPO_DIR%scripts\etl_load.py"
SET "LOG_DIR=%REPO_DIR%logs"
SET "TIMESTAMP=%DATE:/=-%_%TIME::=-%"
SET "TIMESTAMP=%TIMESTAMP: =0%"
SET "LOG_FILE=%LOG_DIR%\etl_run_%TIMESTAMP%.log"

IF NOT EXIST "%LOG_DIR%" mkdir "%LOG_DIR%"

echo ==============================================================
echo Starting ETL Load at %DATE% %TIME%
echo ==============================================================
echo.

REM ===== VALIDATE ENVIRONMENT =====
IF NOT EXIST "%PYTHON_EXE%" (
    echo [ERROR] Python virtual environment not found:
    echo %PYTHON_EXE%
    echo Please run: python -m venv scripts\.venv
    exit /b 1
)

IF NOT EXIST "%SCRIPT_PATH%" (
    echo [ERROR] Could not find etl_load.py at:
    echo %SCRIPT_PATH%
    exit /b 1
)

REM ===== ACTIVATE VENV =====
echo Activating virtual environment...
call "%VENV_DIR%\Scripts\activate.bat"

REM ===== RUN ETL LOAD SCRIPT =====
echo Running etl_load.py...
"%PYTHON_EXE%" "%SCRIPT_PATH%" > "%LOG_FILE%" 2>&1

IF ERRORLEVEL 1 (
    echo [FAILED] ETL run encountered an error. Check the log:
    echo %LOG_FILE%
    echo.
    type "%LOG_FILE%"
    exit /b 1
)

REM ===== SUCCESS =====
echo [SUCCESS] ETL pipeline completed successfully.
echo Log saved to: %LOG_FILE%
echo.
type "%LOG_FILE%"
echo.
pause
exit /b 0

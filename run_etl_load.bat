@echo off
setlocal ENABLEDELAYEDEXPANSION

REM ==============================================================
REM  Azure Healthcare ETL - Full Runner
REM  - Runs SQL setup (00/02/03/04) + Python ETL with logging
REM ==============================================================

REM ===== USER CONFIG =====
SET "REPO_DIR=%~dp0"
SET "SQL_SERVER=sql-kb-KB1992.database.windows.net"
SET "SQL_DB=db-healthops"
SET "SQL_USER=sqladminkb"

REM If you want to be prompted for the password at runtime, set USE_PASSWORD=0.
REM For unattended runs (Task Scheduler), set USE_PASSWORD=1 and fill SQL_PWD.
SET "USE_PASSWORD=0"
SET "SQL_PWD=YOUR_STRONG_PASSWORD"

REM ===== PATHS =====
SET "VENV_DIR=%REPO_DIR%scripts\.venv"
SET "PYTHON_EXE=%VENV_DIR%\Scripts\python.exe"
SET "PY_ACTIVATE=%VENV_DIR%\Scripts\activate.bat"
SET "ETL_SCRIPT=%REPO_DIR%scripts\etl_load.py"

SET "SQL_DIR=%REPO_DIR%sql"
SET "SQL_00=%SQL_DIR%\00_init_db.sql"
SET "SQL_02=%SQL_DIR%\02_upsert_objects.sql"
SET "SQL_03=%SQL_DIR%\03_data_quality.sql"
SET "SQL_04=%SQL_DIR%\04_audit.sql"

SET "LOG_DIR=%REPO_DIR%logs"
IF NOT EXIST "%LOG_DIR%" mkdir "%LOG_DIR%"

for /f "tokens=1-3 delims=/- " %%a in ('date /t') do (set _d=%%c%%a%%b)
for /f "tokens=1-2 delims=: " %%a in ('time /t') do (set _t=%%a%%b)
set "_t=%_t: =0%"
SET "STAMP=%_d%-%_t%"

SET "MASTER_LOG=%LOG_DIR%\run_%STAMP%.log"
SET "L_SQL00=%LOG_DIR%\00_init_db_%STAMP%.out"
SET "L_SQL02=%LOG_DIR%\02_upsert_objects_%STAMP%.out"
SET "L_SQL03=%LOG_DIR%\03_data_quality_%STAMP%.out"
SET "L_SQL04=%LOG_DIR%\04_audit_%STAMP%.out"
SET "L_PYSTD=%LOG_DIR%\python_etl_stdout_%STAMP%.log"
SET "L_PYERR=%LOG_DIR%\python_etl_stderr_%STAMP%.log"

REM ===== HELPERS =====
SET "AUTH_BASE=-S %SQL_SERVER% -d %SQL_DB% -U %SQL_USER% -N -l 30 -b"
IF "%USE_PASSWORD%"=="1" (SET "SQLAUTH=%AUTH_BASE% -P %SQL_PWD%") ELSE (SET "SQLAUTH=%AUTH_BASE%")

CALL :log "========== RUN START %DATE% %TIME% =========="

REM ===== VALIDATE TOOLS & FILES =====
where sqlcmd >nul 2>&1 || CALL :fail "sqlcmd not found in PATH."
IF NOT EXIST "%PYTHON_EXE%" CALL :fail "Python venv not found at %PYTHON_EXE%"
IF NOT EXIST "%ETL_SCRIPT%" CALL :fail "ETL script missing: %ETL_SCRIPT%"
IF NOT EXIST "%SQL_02%" CALL :fail "Missing required SQL file: %SQL_02%"
IF NOT EXIST "%SQL_03%" CALL :fail "Missing required SQL file: %SQL_03%"
IF NOT EXIST "%SQL_04%" CALL :fail "Missing required SQL file: %SQL_04%"

REM ===== SQL: 00 (optional) =====
IF EXIST "%SQL_00%" (
  CALL :log "Running 00_init_db.sql"
  sqlcmd %SQLAUTH% -i "%SQL_00%" -o "%L_SQL00%"
  IF ERRORLEVEL 1 CALL :fail "00_init_db.sql failed. See %L_SQL00%"
) ELSE (
  CALL :log "Skipping 00_init_db.sql (file not present)."
)

REM ===== SQL: 02 upserts =====
CALL :log "Running 02_upsert_objects.sql"
sqlcmd %SQLAUTH% -i "%SQL_02%" -o "%L_SQL02%"
IF ERRORLEVEL 1 CALL :fail "02_upsert_objects.sql failed. See %L_SQL02%"

REM ===== SQL: 03 data quality =====
CALL :log "Running 03_data_quality.sql"
sqlcmd %SQLAUTH% -i "%SQL_03%" -o "%L_SQL03%"
IF ERRORLEVEL 1 CALL :fail "03_data_quality.sql failed. See %L_SQL03%"

REM ===== SQL: 04 audit =====
CALL :log "Running 04_audit.sql"
sqlcmd %SQLAUTH% -i "%SQL_04%" -o "%L_SQL04%"
IF ERRORLEVEL 1 CALL :fail "04_audit.sql failed. See %L_SQL04%"

REM ===== ACTIVATE VENV =====
CALL :log "Activating virtual environment"
CALL "%PY_ACTIVATE%" || CALL :fail "Failed to activate venv: %PY_ACTIVATE%"

REM ===== RUN ETL =====
CALL :log "Running Python ETL: %ETL_SCRIPT%"
"%PYTHON_EXE%" "%ETL_SCRIPT%" 1>>"%L_PYSTD%" 2>>"%L_PYERR%"
IF ERRORLEVEL 1 CALL :fail "etl_load.py failed. See %L_PYERR%"

REM ===== SUCCESS =====
CALL :log "SUCCESS. See logs:"
CALL :log "  %MASTER_LOG%"
CALL :log "  %L_SQL02%"
CALL :log "  %L_SQL03%"
CALL :log "  %L_SQL04%"
CALL :log "  %L_PYSTD%"
CALL :log "========== RUN END %DATE% %TIME% =========="
echo.
type "%L_PYSTD%"
echo.
exit /b 0

:log
  echo [%DATE% %TIME%] %~1
  echo [%DATE% %TIME%] %~1>>"%MASTER_LOG%"
  goto :eof

:fail
  echo [%DATE% %TIME%] ERROR: %~1 1>&2
  echo [%DATE% %TIME%] ERROR: %~1>>"%MASTER_LOG%"
  echo Review logs in: "%LOG_DIR%"
  exit /b 1

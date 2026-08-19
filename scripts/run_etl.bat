@echo OFF
SETLOCAL
:: Scheduled entry point.
::
:: The previous version neither redirected output to a log nor propagated
:: Python's exit code, so Task Scheduler recorded every run as successful --
:: which is why a five-month outage went unnoticed. Both are fixed here.
::
:: Paths are DERIVED, never hardcoded: %~dp0 is this file's own directory, so
:: the repo root is its parent. Nothing below names a user or a machine, which
:: keeps a username out of a public repo and makes the script portable.

FOR %%I IN ("%~dp0..") DO SET "REPO=%%~fI"
SET "LOGDIR=%REPO%\logs"
IF NOT EXIST "%LOGDIR%" MKDIR "%LOGDIR%"

FOR /F %%d IN ('powershell -NoProfile -Command "Get-Date -Format yyyy-MM-dd"') DO SET "TODAY=%%d"
SET "LOGFILE=%LOGDIR%\run_etl-%TODAY%.log"

:: Set CONDA_ROOT in the environment to point at a non-default install.
IF NOT DEFINED CONDA_ROOT SET "CONDA_ROOT=%USERPROFILE%\anaconda3"
IF NOT EXIST "%CONDA_ROOT%\Scripts\activate.bat" (
    ECHO [%DATE% %TIME%] ERROR: conda activate.bat not found under "%CONDA_ROOT%" -- set CONDA_ROOT to your Anaconda install >> "%LOGFILE%"
    EXIT /B 1
)

CALL "%CONDA_ROOT%\Scripts\activate.bat"
CALL conda activate AQI_Predict

python "%REPO%\scripts\fetch.py" >> "%LOGFILE%" 2>&1
SET "RC=%ERRORLEVEL%"
ECHO [%DATE% %TIME%] fetch.py exited with %RC% >> "%LOGFILE%"
EXIT /B %RC%

@echo OFF
SETLOCAL
:: Scheduled entry point.
::
:: The previous version neither redirected output to a log nor propagated
:: Python's exit code, so Task Scheduler recorded every run as successful --
:: which is why a five-month outage went unnoticed. Both are fixed here.

SET "REPO=C:\Users\stant\Documents\Projects\AQI_Predict"
SET "LOGDIR=%REPO%\logs"
IF NOT EXIST "%LOGDIR%" MKDIR "%LOGDIR%"

FOR /F %%d IN ('powershell -NoProfile -Command "Get-Date -Format yyyy-MM-dd"') DO SET "TODAY=%%d"
SET "LOGFILE=%LOGDIR%\run_etl-%TODAY%.log"

CALL C:\Users\stant\anaconda3\Scripts\activate.bat
CALL conda activate AQI_Predict

python "%REPO%\scripts\fetch.py" >> "%LOGFILE%" 2>&1
SET "RC=%ERRORLEVEL%"
ECHO [%DATE% %TIME%] fetch.py exited with %RC% >> "%LOGFILE%"
EXIT /B %RC%

@echo OFF
ECHO Starting daily AQI ETL process...

:: This command initializes Conda in the batch script's environment
call C:\Users\stant\anaconda3\Scripts\activate.bat

:: This command activates your specific project environment
call conda activate AQI_Predict

:: This command runs your python script using its full path
python C:\Users\stant\Documents\Projects\AQI_Predict\scripts\etl.py

ECHO ETL process finished.
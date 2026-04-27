@echo off
REM Launcher for the DataCycle admin dashboard.
REM Double-click this file to open it in your browser.
cd /d "%~dp0\.."
if exist ".venv\Scripts\python.exe" (
    .venv\Scripts\python.exe -m streamlit run scripts\admin.py
) else (
    python -m streamlit run scripts\admin.py
)
pause

@echo off
setlocal
cd /d "%~dp0"

REM Use the project-local virtual environment. If it does not exist, create it.
set "PYEXE=%~dp0.venv\Scripts\python.exe"

if not exist "%PYEXE%" (
  echo [setup] Creating project virtual environment...
  python -m venv ".venv" || (echo [ERROR] Could not create virtual environment. & pause & exit /b 1)
)

echo [setup] Checking Python dependencies...
"%PYEXE%" -m pip install -r requirements.txt || (echo [ERROR] Could not install dependencies. & pause & exit /b 1)

"%PYEXE%" app.py

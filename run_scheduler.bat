@echo off
setlocal
cd /d "%~dp0"

REM Usa o venv do proprio projeto. Se nao existir, cria e instala as deps.
set "PYEXE=%~dp0.venv\Scripts\python.exe"

if not exist "%PYEXE%" (
  echo [setup] Criando venv do projeto...
  python -m venv ".venv" || (echo [ERRO] Falha ao criar venv. & pause & exit /b 1)
  "%PYEXE%" -m pip install -r requirements.txt || (echo [ERRO] Falha ao instalar dependencias. & pause & exit /b 1)
)

"%PYEXE%" app.py

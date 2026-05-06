@echo off
cd /d "%~dp0"
echo ==================================================
echo   SINCRONIZAR DATOS DESDE SUPABASE
echo   Este script descarga datos y los guarda en SQLite local
echo ==================================================
echo.
.venv\Scripts\python.exe sync_supabase.py
echo.
pause
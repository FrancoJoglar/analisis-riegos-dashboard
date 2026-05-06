@echo off
cd /d "%~dp0"
echo ==================================================
echo   ANALISIS RIEGOS 2025-2026
echo   Dashboard Interactivo
echo ==================================================
echo.
echo Abriendo http://localhost:8501 en el navegador...
echo.
start http://localhost:8501
python\python.exe -m streamlit run app.py
pause

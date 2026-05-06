@echo off
cd /d "%~dp0"
echo ==================================================
echo   ANALISIS RIEGOS 2025-2026
echo   Dashboard Interactivo
echo ==================================================
echo.
echo Abriendo en el navegador...
echo Cierra la ventana del navegador para detener.
echo.
python\python.exe -m streamlit run app.py --server.headless true
pause
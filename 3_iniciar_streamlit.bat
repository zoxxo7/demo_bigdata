@echo off
title [3/3] STREAMLIT DASHBOARD - NOC CENTRO DE CONTROL
color 0B
cd /d "%~dp0"
set "PY=C:\Users\ZAFNAT MAMANI\AppData\Local\Python\bin\python.exe"

echo ======================================================================
echo    [3/3] DASHBOARD WEB STREAMLIT + FOLIUM (PUERTO 8501)
echo ======================================================================
echo.
"%PY%" -m streamlit run app.py --server.port 8501 --browser.gatherUsageStats false
echo.
echo Streamlit se ha detenido.
pause

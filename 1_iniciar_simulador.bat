@echo off
title [1/3] SIMULADOR NOC - 62 TORRES BOLIVIA
color 0A
cd /d "%~dp0"
set "PY=C:\Users\ZAFNAT MAMANI\AppData\Local\Python\bin\python.exe"

echo ======================================================================
echo    [1/3] GENERADOR DE TELEMETRIA EN TIEMPO REAL (62 TORRES)
echo ======================================================================
echo.
"%PY%" simulador.py
echo.
echo El simulador se ha detenido.
pause

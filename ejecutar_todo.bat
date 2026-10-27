@echo off
title NOC BOLIVIA - LANZADOR MAESTRO
color 0F
cls
cd /d "%~dp0"

echo ======================================================================
echo    BIG DATA NOC BOLIVIA - CENTRO DE CONTROL NACIONAL
echo    Lanzando los 3 componentes en ventanas independientes...
echo ======================================================================
echo.

if not exist "stream_input" mkdir "stream_input"
if not exist "stream_output" mkdir "stream_output"
if not exist "spark_checkpoint" mkdir "spark_checkpoint"

echo Limpiando datos antiguos de streaming...
del /q /f stream_input\*.* 2>nul
del /q /f stream_output\*.* 2>nul

echo [1/3] Abriendo Simulador de Telemetria...
start "" "%~dp01_iniciar_simulador.bat"
timeout /t 3 /nobreak >nul

echo [2/3] Abriendo Apache PySpark Streaming...
start "" "%~dp02_iniciar_spark.bat"
timeout /t 6 /nobreak >nul

echo [3/3] Abriendo Dashboard Web Streamlit...
start "" "%~dp03_iniciar_streamlit.bat"
timeout /t 4 /nobreak >nul

echo Abriendo navegador en http://localhost:8501...
start http://localhost:8501

echo.
echo ======================================================================
echo    TODO HA SIDO INICIADO EXITOSAMENTE
echo.
echo    Tienes 3 ventanas abiertas en tu barra de tareas:
echo      1. [1/3] SIMULADOR NOC (Verde)
echo      2. [2/3] APACHE PYSPARK (Amarillo)
echo      3. [3/3] STREAMLIT DASHBOARD (Azul)
echo.
echo    Y tu navegador web se ha abierto con el mapa y graficos.
echo ======================================================================
echo.
echo Presiona cualquier tecla para cerrar este lanzador...
pause >nul

@echo off
title [2/3] APACHE PYSPARK STREAMING - MOTOR BIG DATA
color 0E
cd /d "%~dp0"
set "PY=C:\Users\ZAFNAT MAMANI\AppData\Local\Python\bin\python.exe"
set "JAVA_HOME=C:\Program Files\Microsoft\jdk-21.0.13.21-hotspot"
set "HADOOP_HOME=C:\hadoop"
set "PYSPARK_PYTHON=%PY%"
set "PYSPARK_DRIVER_PYTHON=%PY%"
set "PATH=%JAVA_HOME%\bin;C:\hadoop\bin;%PATH%"

echo ======================================================================
echo    [2/3] APACHE PYSPARK STRUCTURED STREAMING (24 CORES LOCAL)
echo ======================================================================
echo.
"%PY%" spark_processor.py
echo.
echo PySpark se ha detenido.
pause

Write-Host "======================================================================" -ForegroundColor Cyan
Write-Host "   BIG DATA NOC DEMO - REDES DE TELECOMUNICACIONES BOLIVIA" -ForegroundColor Cyan
Write-Host "   Apache PySpark 4.2 Streaming + Streamlit + Folium (62 Torres)" -ForegroundColor Cyan
Write-Host "======================================================================" -ForegroundColor Cyan
Write-Host ""

$PY = "C:\Users\ZAFNAT MAMANI\AppData\Local\Python\bin\python.exe"
$JAVA_HOME = "C:\Program Files\Microsoft\jdk-21.0.13.21-hotspot"
$env:JAVA_HOME = $JAVA_HOME
$env:HADOOP_HOME = "C:\hadoop"
$env:PYSPARK_PYTHON = $PY
$env:PYSPARK_DRIVER_PYTHON = $PY
$env:PATH = "$JAVA_HOME\bin;C:\hadoop\bin;" + $env:PATH

Set-Location -Path $PSScriptRoot

New-Item -ItemType Directory -Force -Path "stream_input", "stream_output", "spark_checkpoint" | Out-Null
Remove-Item -Path "stream_input\*", "stream_output\*", "spark_checkpoint\*" -Recurse -Force -ErrorAction SilentlyContinue

Write-Host "[1/3] Lanzando: Simulador de Telemetria (62 Torres)..." -ForegroundColor Green
Start-Process powershell -ArgumentList "-NoExit", "-Command", "`$host.UI.RawUI.WindowTitle = '[1/3] SIMULADOR NOC (Telemetria)'; Set-Location '$PSScriptRoot'; & '$PY' simulador.py"

Start-Sleep -Seconds 3

Write-Host "[2/3] Lanzando: Apache PySpark Streaming..." -ForegroundColor Yellow
Start-Process powershell -ArgumentList "-NoExit", "-Command", "`$host.UI.RawUI.WindowTitle = '[2/3] SPARK PROCESSOR (Streaming Engine)'; Set-Location '$PSScriptRoot'; `$env:JAVA_HOME = '$JAVA_HOME'; `$env:HADOOP_HOME = 'C:\hadoop'; `$env:PYSPARK_PYTHON = '$PY'; `$env:PYSPARK_DRIVER_PYTHON = '$PY'; `$env:PATH = '$JAVA_HOME\bin;C:\hadoop\bin;' + `$env:PATH; & '$PY' spark_processor.py"

Start-Sleep -Seconds 6

Write-Host "[3/3] Lanzando: Servidor Web Streamlit..." -ForegroundColor Magenta
Start-Process powershell -ArgumentList "-NoExit", "-Command", "`$host.UI.RawUI.WindowTitle = '[3/3] STREAMLIT DASHBOARD (Web)'; Set-Location '$PSScriptRoot'; & '$PY' -m streamlit run app.py --server.port 8501"

Write-Host ""
Write-Host "======================================================================" -ForegroundColor Green
Write-Host "   SISTEMA NOC INICIADO CORRECTAMENTE" -ForegroundColor Green
Write-Host "   Las 3 ventanas independientes estan abiertas en tu escritorio:" -ForegroundColor Green
Write-Host "     1. [1/3] SIMULADOR NOC" -ForegroundColor Green
Write-Host "     2. [2/3] SPARK PROCESSOR (Apache PySpark)" -ForegroundColor Green
Write-Host "     3. [3/3] STREAMLIT DASHBOARD (Web en localhost:8501)" -ForegroundColor Green
Write-Host "======================================================================" -ForegroundColor Green

Start-Sleep -Seconds 4
Start-Process "http://localhost:8501"

Write-Host ""
Read-Host "Presiona Enter para cerrar este lanzador..."

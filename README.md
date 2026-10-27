# 📡 Big Data NOC — Monitoreo de Telecomunicaciones Bolivia

> **Proyecto Universitario — 7mo Semestre (Tecnologías Emergentes)**  
> Sistema Distribuido en Tiempo Real para Detección Proactiva de Fallas en Redes Celulares (62 Torres en los 9 Departamentos de Bolivia) utilizando **Apache PySpark Structured Streaming**, **Streamlit** y **Folium**.

---

## 🏗️ 1. Arquitectura del Sistema (Pipeline Big Data)

El sistema implementa una **Arquitectura Streaming de Baja Latencia** dividida en 3 capas desacopladas:

```
┌─────────────────────────────────┐
│        1. CAPA DE INGESTA       │
│          simulador.py           │ ──► Genera telemetría continua (17 métricas)
│  (62 Torres en 9 departamentos) │     en formato NDJSON cada 1 segundo.
└────────────────┬────────────────┘
                 │ escribe en
                 ▼
┌─────────────────────────────────┐
│       2. BUFFER DE EVENTOS      │
│          stream_input/          │ ──► Buffer persistente desacoplado.
└────────────────┬────────────────┘
                 │ lee por micro-batches
                 ▼
┌─────────────────────────────────┐
│   3. MOTOR DISTRIBUIDO SPARK    │
│       spark_processor.py        │ ──► Apache PySpark Structured Streaming
│ (24 núcleos CPU en paralelo)    │     - Inferencia de Causa Raíz
│                                 │     - Cálculo de Score de Salud (0-100)
│                                 │     - Spark Web UI: http://localhost:4040
└────────────────┬────────────────┘
                 │ escribe particiones
                 ▼
┌─────────────────────────────────┐
│       4. STORAGE ANALÍTICO      │
│         stream_output/          │ ──► Resultados consolidados por batch.
└────────────────┬────────────────┘
                 │ lee y actualiza
                 ▼
┌─────────────────────────────────┐
│     5. CAPA DE PRESENTACIÓN     │
│             app.py              │ ──► Dashboard NOC en Streamlit + Folium
│      http://localhost:8501      │     - Mapa interactivo con memoria de zoom
│                                 │     - Filtros por Depto, Tecnología y Severidad
└─────────────────────────────────┘
```

---

## 💎 2. Justificación de las 5 V's de Big Data

| Dimensión | Justificación en el Proyecto |
| :--- | :--- |
| **🚀 Velocidad** | Ingesta masiva cada 1s y procesamiento por micro-batches en sub-segundos con **Apache PySpark Streaming**. |
| **📦 Volumen** | Monitoreo simultáneo de **62 nodos** distribuidos a nivel nacional, superando 220,000 puntos de telemetría por hora sin degradación de memoria. |
| **🔀 Variedad** | Datos semiestructurados (JSON), telemetría de radiofrecuencia (dBm), capas físicas (fibra/microondas), coordenadas geoespaciales y estados operativos. |
| **🛡️ Veracidad** | Validación estricta de esquema (`StructType`), filtrado de anomalías espurias y algoritmo de diagnóstico multidimensional de causa raíz. |
| **💡 Valor** | Detección temprana de incidentes (cortes de fibra, sobrecalentamiento, congestión) antes de que afecten a los abonados (SLA 99.9%). |

---

## 🚀 3. Inicio Rápido en Windows

### Opción A — Un solo clic (Recomendado)
Haz doble clic en:
```bat
ejecutar_todo.bat
```
Esto abrirá automáticamente las 3 consolas con código de color y lanzará el navegador en `http://localhost:8501`.

### Opción B — Modular (Paso a Paso)
1. **Consola 1 (Verde):** `1_iniciar_simulador.bat`
2. **Consola 2 (Amarilla):** `2_iniciar_spark.bat`
3. **Consola 3 (Azul):** `3_iniciar_streamlit.bat`
4. Abrir en el navegador: **`http://localhost:8501`**

---

## 🔍 4. Dónde ver Apache Spark

* **Consola de Procesamiento:** En la ventana de `spark_processor.py` verás los micro-batches procesados, cantidad de registros y estados.
* **Spark Web UI Oficial:** Mientras PySpark esté activo, abre en tu navegador:
  👉 **`http://localhost:4040`**  
  Allí podrás mostrarle al docente los *Jobs*, *Stages*, *Executors* y el uso de los núcleos del procesador.

---

## 👥 5. Cómo Compartir el Proyecto con Otros (para VS Code)

### Si lo compartes por GitHub o Git:
1. Sube el repositorio. El archivo `.gitignore` ya está configurado para omitir archivos temporales generados por el streaming.
2. Tu compañero o docente solo debe clonarlo:
   ```bash
   git clone <URL_DEL_REPOSITORIO>
   cd demo_bigdata
   ```

### Si lo compartes en archivo ZIP:
1. Comprime toda la carpeta del proyecto **excepto** `stream_input`, `stream_output` y `spark_checkpoint`.
2. El destinatario descomprime la carpeta y la abre en Visual Studio Code.

### Pasos que debe hacer la otra persona en su VS Code:
1. Abrir la terminal integrada en VS Code (`Ctrl + ñ`).
2. Instalar las dependencias de Python:
   ```bash
   pip install -r requirements.txt
   ```
3. Ejecutar el proyecto:
   * En Windows: Hacer doble clic o ejecutar `./ejecutar_todo.bat` en la terminal.
   * O correr los 3 scripts por separado en terminales de VS Code:
     - Terminal 1: `python simulador.py`
     - Terminal 2: `python spark_processor.py`
     - Terminal 3: `streamlit run app.py`

*(Nota: Para ejecutar PySpark en su máquina, la persona debe tener instalado Java JDK 11 o superior y Python 3.10+).*

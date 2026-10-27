"""
spark_processor.py — Motor de procesamiento distribuido masivo con PySpark Structured Streaming
Monitorea 62 torres en 9 departamentos de Bolivia, evalúa telemetría multidimensional,
diagnostica causa raíz de fallas en tiempo real y persiste en stream_output/.
"""

import os
import sys
import logging
import glob
from datetime import datetime

# Forzar UTF-8 en stdout/stderr para consolas Windows (evita UnicodeEncodeError cp1252)
if sys.platform.startswith("win"):
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

# ─────────────────────────────────────────────
# AUTO-DETECCIÓN DE JAVA_HOME (Windows)
# ─────────────────────────────────────────────
def _detectar_java() -> None:
    """Configura JAVA_HOME automáticamente si no está en el entorno."""
    if os.environ.get("JAVA_HOME"):
        return

    candidatos = [
        r"C:\Program Files\Microsoft\jdk-21.0.13.21-hotspot",
        r"C:\Program Files\Microsoft\jdk-21*",
        r"C:\Program Files\Microsoft\jdk-17*",
        r"C:\Program Files\Microsoft\jdk-11*",
        r"C:\Program Files\Eclipse Adoptium\jdk-*",
        r"C:\Program Files\Java\jdk-*",
    ]
    for patron in candidatos:
        rutas = sorted(glob.glob(patron), reverse=True)
        for ruta in rutas:
            java_exe = os.path.join(ruta, "bin", "java.exe")
            if os.path.isfile(java_exe):
                os.environ["JAVA_HOME"] = ruta
                os.environ["PATH"] = os.path.join(ruta, "bin") + os.pathsep + os.environ.get("PATH", "")
                print(f"  Java detectado: {ruta}")
                return

_detectar_java()

# ─────────────────────────────────────────────
# FIJAR PYTHON WORKER Y HADOOP
# ─────────────────────────────────────────────
def _configurar_python_worker() -> None:
    ejecutables = [
        r"C:\Users\ZAFNAT MAMANI\AppData\Local\Python\bin\python.exe",
        sys.executable,
        r"C:\Users\ZAFNAT MAMANI\AppData\Local\Python\pythoncore-3.14-64\python.exe",
    ]
    for c in ejecutables:
        if c and os.path.isfile(c) and "WindowsApps" not in c:
            os.environ["PYSPARK_PYTHON"] = c
            os.environ["PYSPARK_DRIVER_PYTHON"] = c
            break

    if not os.environ.get("HADOOP_HOME"):
        winutils_path = r"C:\hadoop"
        if os.path.isfile(os.path.join(winutils_path, "bin", "winutils.exe")):
            os.environ["HADOOP_HOME"] = winutils_path
            os.environ["PATH"] = (
                os.path.join(winutils_path, "bin") + os.pathsep +
                os.environ.get("PATH", "")
            )

_configurar_python_worker()

# Suprimir logs verbosos
logging.getLogger("py4j").setLevel(logging.ERROR)

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType, BooleanType, LongType
)

# ─────────────────────────────────────────────
# RUTAS Y UMBRALES
# ─────────────────────────────────────────────
INPUT_DIR      = "stream_input"
OUTPUT_DIR     = "stream_output"
CHECKPOINT_DIR = "spark_checkpoint"

os.makedirs(INPUT_DIR,      exist_ok=True)
os.makedirs(OUTPUT_DIR,     exist_ok=True)
os.makedirs(CHECKPOINT_DIR, exist_ok=True)

UMBRAL_LATENCIA_CRITICO  = 250.0
UMBRAL_LATENCIA_ALERTA   = 120.0
UMBRAL_PERDIDA_CRITICO   =  30.0
UMBRAL_PERDIDA_ALERTA    =  10.0

# ─────────────────────────────────────────────
# SCHEMA ENRIQUECIDO DE TELEMETRÍA (17 campos)
# ─────────────────────────────────────────────
SCHEMA_TELEMETRIA = StructType([
    StructField("torre_id",            StringType(),  True),
    StructField("nombre",              StringType(),  True),
    StructField("lat",                 DoubleType(),  True),
    StructField("lon",                 DoubleType(),  True),
    StructField("depto",               StringType(),  True),
    StructField("tecnologia",          StringType(),  True),
    StructField("frec_mhz",            DoubleType(),  True),
    StructField("latencia_ms",         DoubleType(),  True),
    StructField("perdida_pct",         DoubleType(),  True),
    StructField("trafico_mbps",        DoubleType(),  True),
    StructField("jitter_ms",           DoubleType(),  True),
    StructField("temperatura_c",       DoubleType(),  True),
    StructField("usuarios_conectados", LongType(),    True),
    StructField("senal_dbm",           DoubleType(),  True),
    StructField("cpu_pct",             DoubleType(),  True),
    StructField("timestamp",           StringType(),  True),
    StructField("es_anomalia",         BooleanType(), True),
    StructField("tipo_falla",          StringType(),  True),
])


def crear_sesion_spark() -> SparkSession:
    """Inicializa SparkSession optimizado para alto rendimiento local (24 núcleos)."""
    spark = (
        SparkSession.builder
        .appName("NOC_Telecom_Bolivia_62Torres")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.streaming.checkpointLocation", CHECKPOINT_DIR)
        .config("spark.driver.memory", "3g")
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.hadoop.io.nativeio.NativeIO.Windows.access0", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def clasificar_estado(latencia: F.Column, perdida: F.Column) -> F.Column:
    """Clasificación operativa SLA: CRÍTICO | ALERTA | ÓPTIMO"""
    return (
        F.when(
            (latencia > UMBRAL_LATENCIA_CRITICO) | (perdida > UMBRAL_PERDIDA_CRITICO),
            F.lit("CRÍTICO")
        )
        .when(
            (latencia > UMBRAL_LATENCIA_ALERTA) | (perdida > UMBRAL_PERDIDA_ALERTA),
            F.lit("ALERTA")
        )
        .otherwise(F.lit("ÓPTIMO"))
    )


def diagnosticar_falla(temp: F.Column, lat: F.Column, perdida: F.Column,
                       users: F.Column, senal: F.Column, estado: F.Column) -> F.Column:
    """Motor de reglas de causa raíz multidimensional para el NOC."""
    return (
        F.when(temp > 70.0, F.lit("Sobrecalentamiento Físico"))
        .when((lat > 280.0) & (perdida > 35.0), F.lit("Corte de Enlace / Fibra"))
        .when((users > 850) & (lat > 120.0), F.lit("Congestión de Celdas (Demanda)"))
        .when(senal < -92.0, F.lit("Atenuación Radio / Tormenta"))
        .when(estado == "CRÍTICO", F.lit("Falla Severa SLA"))
        .when(estado == "ALERTA", F.lit("Degradación en Observación"))
        .otherwise(F.lit("Operación Nominal"))
    )


def calcular_score_salud(latencia: F.Column, perdida: F.Column,
                         trafico: F.Column, cpu: F.Column,
                         jitter: F.Column, temp: F.Column) -> F.Column:
    """Score de salud ponderado 0-100 considerando red + hardware."""
    score_lat  = F.greatest(F.lit(0.0), F.lit(100.0) - (latencia / 7.0))
    score_perd = F.greatest(F.lit(0.0), F.lit(100.0) - (perdida * 2.5))
    score_cpu  = F.greatest(F.lit(0.0), F.lit(100.0) - cpu)
    score_jit  = F.greatest(F.lit(0.0), F.lit(100.0) - (jitter * 2.0))
    score_temp = F.greatest(F.lit(0.0), F.lit(100.0) - F.greatest(F.lit(0.0), (temp - 30.0) * 1.8))

    return F.round(
        (score_lat * 0.30) + (score_perd * 0.30) +
        (score_temp * 0.15) + (score_cpu * 0.15) + (score_jit * 0.10),
        1
    )


def procesar_stream(spark: SparkSession) -> None:
    """Pipeline masivo de Structured Streaming."""
    print("  Leyendo stream masivo de telemetría desde:", INPUT_DIR)

    raw_stream = (
        spark.readStream
        .format("json")
        .schema(SCHEMA_TELEMETRIA)
        .option("maxFilesPerTrigger", 10)
        .load(INPUT_DIR)
    )

    procesado = (
        raw_stream
        .withColumn("evento_ts",
            F.to_timestamp(F.col("timestamp")))
        .withColumn("estado",
            clasificar_estado(F.col("latencia_ms"), F.col("perdida_pct")))
        .withColumn("diagnostico",
            diagnosticar_falla(
                F.col("temperatura_c"), F.col("latencia_ms"), F.col("perdida_pct"),
                F.col("usuarios_conectados"), F.col("senal_dbm"), F.col("estado")
            ))
        .withColumn("score_salud",
            calcular_score_salud(
                F.col("latencia_ms"), F.col("perdida_pct"),
                F.col("trafico_mbps"), F.col("cpu_pct"),
                F.col("jitter_ms"), F.col("temperatura_c")
            ))
        .withColumn("requiere_atencion",
            F.col("estado").isin("CRÍTICO", "ALERTA"))
        .withColumn("procesado_ts",
            F.current_timestamp())
        .withColumn("hora_evento",
            F.date_format(F.col("evento_ts"), "HH:mm:ss"))
        .select(
            "torre_id", "nombre", "depto", "tecnologia", "frec_mhz",
            "lat", "lon",
            "latencia_ms", "perdida_pct", "trafico_mbps",
            "jitter_ms", "temperatura_c", "usuarios_conectados",
            "senal_dbm", "cpu_pct",
            "estado", "diagnostico", "score_salud",
            "requiere_atencion", "es_anomalia",
            "hora_evento", "procesado_ts"
        )
    )

    # Función de procesamiento por micro-batch (foreachBatch)
    def procesar_micro_batch(batch_df, batch_id: int):
        total = batch_df.count()
        if total > 0:
            # 1. Persistir particiones CSV para el Dashboard
            (
                batch_df.write
                .mode("append")
                .option("header", "true")
                .csv(OUTPUT_DIR)
            )

            # 2. Resumen visual en la consola de Spark para la defensa
            criticos = batch_df.filter(F.col("estado") == "CRÍTICO").count()
            alertas  = batch_df.filter(F.col("estado") == "ALERTA").count()
            optimos  = total - criticos - alertas

            hora = datetime.now().strftime("%H:%M:%S")
            print(f"[{hora}] ⚡ Micro-Batch #{batch_id:04d} procesado ({total} torres) → "
                  f"🔴 Críticos: {criticos} | 🟡 Alertas: {alertas} | 🟢 Óptimos: {optimos}")

    query = (
        procesado.writeStream
        .foreachBatch(procesar_micro_batch)
        .option("checkpointLocation", os.path.join(CHECKPOINT_DIR, "engine_v1"))
        .trigger(processingTime="2 seconds")
        .start()
    )

    print("  Streaming masivo activo (62 torres). Procesando micro-batches cada 2s...")
    print("  Resultados en stream_output/  |  Checkpoint en spark_checkpoint/\n")

    query.awaitTermination()


if __name__ == "__main__":
    print("=" * 75)
    print("  SPARK STRUCTURED STREAMING MASIVO — 62 TORRES BOLIVIA (9 DEPTOS)")
    print("=" * 75)
    print(f"  Input  : ./{INPUT_DIR}/")
    print(f"  Output : ./{OUTPUT_DIR}/")
    print(f"  Checkpoint: ./{CHECKPOINT_DIR}/")
    print("=" * 75)

    try:
        spark = crear_sesion_spark()
        print(f"  SparkSession lista | Cores paralelos: {spark.sparkContext.defaultParallelism}\n")
        procesar_stream(spark)
    except KeyboardInterrupt:
        print("\n  Procesador Spark detenido.")
    except Exception as e:
        print(f"\n  Error en Spark Processor: {e}")
        raise

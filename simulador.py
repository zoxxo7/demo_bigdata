"""
simulador.py — Generador de telemetría masiva en streaming para NOC (Bolivia)
Simula 62 torres de telecomunicaciones en los 9 departamentos emitiendo JSON cada segundo.
Inyecta anomalías multidimensionales periódicas (congestión, corte de enlace, sobrecalentamiento).
"""

import sys
import io
import json
import os
import random
import time
from datetime import datetime, timezone

# Forzar UTF-8 en stdout/stderr para consolas Windows (evita UnicodeEncodeError cp1252)
if sys.platform.startswith("win"):
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

# ─────────────────────────────────────────────
# CATÁLOGO DE 62 TORRES (9 Departamentos de Bolivia)
# ─────────────────────────────────────────────
TORRES = [
    # ── BENI (6) ────────────────────────────────────────────────────────
    {"torre_id": "BEN-001", "nombre": "Trinidad Centro - Plaza Ballivián", "lat": -14.8347, "lon": -64.9042, "depto": "Beni", "tecnologia": "4G LTE", "frec_mhz": 700},
    {"torre_id": "BEN-002", "nombre": "Trinidad Campus UABJB",           "lat": -14.8210, "lon": -64.8950, "depto": "Beni", "tecnologia": "5G NR",  "frec_mhz": 3500},
    {"torre_id": "BEN-003", "nombre": "Trinidad Aeropuerto J. Henrich",   "lat": -14.8194, "lon": -64.9178, "depto": "Beni", "tecnologia": "4G LTE", "frec_mhz": 1900},
    {"torre_id": "BEN-004", "nombre": "Riberalta Norte Amazonía",         "lat": -10.9833, "lon": -66.1000, "depto": "Beni", "tecnologia": "4G LTE", "frec_mhz": 700},
    {"torre_id": "BEN-005", "nombre": "Guayaramerín Frontera Mamoré",     "lat": -10.8361, "lon": -65.3614, "depto": "Beni", "tecnologia": "Microondas", "frec_mhz": 1900},
    {"torre_id": "BEN-006", "nombre": "San Borja Ganadera",               "lat": -14.8583, "lon": -66.8500, "depto": "Beni", "tecnologia": "4G LTE", "frec_mhz": 700},

    # ── SANTA CRUZ (12) ────────────────────────────────────────────────
    {"torre_id": "SCZ-001", "nombre": "SCZ Equipetrol Empresarial",      "lat": -17.7689, "lon": -63.1932, "depto": "Santa Cruz", "tecnologia": "5G NR",  "frec_mhz": 3500},
    {"torre_id": "SCZ-002", "nombre": "SCZ Plan 3000 Comercial",          "lat": -17.8285, "lon": -63.1342, "depto": "Santa Cruz", "tecnologia": "4G LTE", "frec_mhz": 1900},
    {"torre_id": "SCZ-003", "nombre": "SCZ Parque Industrial",            "lat": -17.7542, "lon": -63.1415, "depto": "Santa Cruz", "tecnologia": "4G LTE", "frec_mhz": 2600},
    {"torre_id": "SCZ-004", "nombre": "SCZ Villa 1ro de Mayo",           "lat": -17.7950, "lon": -63.1400, "depto": "Santa Cruz", "tecnologia": "4G LTE", "frec_mhz": 700},
    {"torre_id": "SCZ-005", "nombre": "SCZ Urubó Residencial",            "lat": -17.7520, "lon": -63.2200, "depto": "Santa Cruz", "tecnologia": "5G NR",  "frec_mhz": 3500},
    {"torre_id": "SCZ-006", "nombre": "Montero Centro Cañero",            "lat": -17.3411, "lon": -63.2558, "depto": "Santa Cruz", "tecnologia": "4G LTE", "frec_mhz": 1900},
    {"torre_id": "SCZ-007", "nombre": "Warnes Parque Logístico",          "lat": -17.5167, "lon": -63.1667, "depto": "Santa Cruz", "tecnologia": "4G LTE", "frec_mhz": 2600},
    {"torre_id": "SCZ-008", "nombre": "Cotoca Santuario",                 "lat": -17.7539, "lon": -62.9986, "depto": "Santa Cruz", "tecnologia": "4G LTE", "frec_mhz": 700},
    {"torre_id": "SCZ-009", "nombre": "Camiri Petrolera",                 "lat": -20.0392, "lon": -63.5186, "depto": "Santa Cruz", "tecnologia": "Microondas", "frec_mhz": 1900},
    {"torre_id": "SCZ-010", "nombre": "Puerto Suárez Frontera",           "lat": -18.9667, "lon": -57.8000, "depto": "Santa Cruz", "tecnologia": "Microondas", "frec_mhz": 700},
    {"torre_id": "SCZ-011", "nombre": "San Ignacio de Velasco Chiquitos", "lat": -16.3667, "lon": -60.9500, "depto": "Santa Cruz", "tecnologia": "4G LTE", "frec_mhz": 700},
    {"torre_id": "SCZ-012", "nombre": "Vallegrande Valles",               "lat": -18.4897, "lon": -64.1064, "depto": "Santa Cruz", "tecnologia": "Microondas", "frec_mhz": 1900},

    # ── LA PAZ (12) ────────────────────────────────────────────────────
    {"torre_id": "LPZ-001", "nombre": "LPZ El Prado Gubernamental",       "lat": -16.5005, "lon": -68.1328, "depto": "La Paz", "tecnologia": "5G NR",  "frec_mhz": 3500},
    {"torre_id": "LPZ-002", "nombre": "LPZ Sopocachi Cultural",           "lat": -16.5122, "lon": -68.1275, "depto": "La Paz", "tecnologia": "4G LTE", "frec_mhz": 1900},
    {"torre_id": "LPZ-003", "nombre": "LPZ Miraflores Hospitales",        "lat": -16.4980, "lon": -68.1180, "depto": "La Paz", "tecnologia": "4G LTE", "frec_mhz": 2600},
    {"torre_id": "LPZ-004", "nombre": "LPZ San Miguel Zona Sur",          "lat": -16.5392, "lon": -68.0803, "depto": "La Paz", "tecnologia": "5G NR",  "frec_mhz": 3500},
    {"torre_id": "LPZ-005", "nombre": "LPZ Achumani Valle del Sol",       "lat": -16.5350, "lon": -68.0650, "depto": "La Paz", "tecnologia": "4G LTE", "frec_mhz": 700},
    {"torre_id": "LPZ-006", "nombre": "El Alto Ceja Distribuidor",        "lat": -16.5100, "lon": -68.1600, "depto": "La Paz", "tecnologia": "4G LTE", "frec_mhz": 2600},
    {"torre_id": "LPZ-007", "nombre": "El Alto Senkata Industrial",       "lat": -16.5650, "lon": -68.1750, "depto": "La Paz", "tecnologia": "4G LTE", "frec_mhz": 1900},
    {"torre_id": "LPZ-008", "nombre": "El Alto Aeropuerto El Alto",       "lat": -16.5133, "lon": -68.1922, "depto": "La Paz", "tecnologia": "5G NR",  "frec_mhz": 3500},
    {"torre_id": "LPZ-009", "nombre": "Viacha Ferroviaria",               "lat": -16.6500, "lon": -68.3000, "depto": "La Paz", "tecnologia": "4G LTE", "frec_mhz": 700},
    {"torre_id": "LPZ-010", "nombre": "Copacabana Lago Titicaca",         "lat": -16.1667, "lon": -69.0833, "depto": "La Paz", "tecnologia": "Microondas", "frec_mhz": 1900},
    {"torre_id": "LPZ-011", "nombre": "Caranavi Yungas Cafetalero",       "lat": -15.8200, "lon": -67.5700, "depto": "La Paz", "tecnologia": "Microondas", "frec_mhz": 700},
    {"torre_id": "LPZ-012", "nombre": "Patacamaya Altiplano Central",     "lat": -17.2333, "lon": -67.9167, "depto": "La Paz", "tecnologia": "4G LTE", "frec_mhz": 700},

    # ── COCHABAMBA (8) ─────────────────────────────────────────────────
    {"torre_id": "CBB-001", "nombre": "CBB Cala Cala Norte",              "lat": -17.3750, "lon": -66.1600, "depto": "Cochabamba", "tecnologia": "5G NR",  "frec_mhz": 3500},
    {"torre_id": "CBB-002", "nombre": "CBB La Cancha Mercado Sur",        "lat": -17.4020, "lon": -66.1580, "depto": "Cochabamba", "tecnologia": "4G LTE", "frec_mhz": 2600},
    {"torre_id": "CBB-003", "nombre": "Quillacollo Santuario Urcupiña",   "lat": -17.3980, "lon": -66.2790, "depto": "Cochabamba", "tecnologia": "4G LTE", "frec_mhz": 1900},
    {"torre_id": "CBB-004", "nombre": "Sacaba Valle Alto",                "lat": -17.4042, "lon": -66.0400, "depto": "Cochabamba", "tecnologia": "4G LTE", "frec_mhz": 1900},
    {"torre_id": "CBB-005", "nombre": "Tiquipaya Ciudad de las Flores",   "lat": -17.3381, "lon": -66.2178, "depto": "Cochabamba", "tecnologia": "4G LTE", "frec_mhz": 700},
    {"torre_id": "CBB-006", "nombre": "Punata Valle Alto",                "lat": -17.5458, "lon": -65.8347, "depto": "Cochabamba", "tecnologia": "Microondas", "frec_mhz": 700},
    {"torre_id": "CBB-007", "nombre": "Colcapirhua Integración",          "lat": -17.3917, "lon": -66.2361, "depto": "Cochabamba", "tecnologia": "4G LTE", "frec_mhz": 2600},
    {"torre_id": "CBB-008", "nombre": "Villa Tunari Trópico Chapare",     "lat": -16.9744, "lon": -65.4217, "depto": "Cochabamba", "tecnologia": "Microondas", "frec_mhz": 1900},

    # ── ORURO (5) ──────────────────────────────────────────────────────
    {"torre_id": "ORU-001", "nombre": "Oruro Plaza Castro de Padilla",     "lat": -17.9667, "lon": -67.1167, "depto": "Oruro", "tecnologia": "4G LTE", "frec_mhz": 1900},
    {"torre_id": "ORU-002", "nombre": "Oruro Zona Sud Minera",            "lat": -17.9900, "lon": -67.1050, "depto": "Oruro", "tecnologia": "4G LTE", "frec_mhz": 700},
    {"torre_id": "ORU-003", "nombre": "Oruro Terminal Terrestre",         "lat": -17.9550, "lon": -67.1080, "depto": "Oruro", "tecnologia": "5G NR",  "frec_mhz": 3500},
    {"torre_id": "ORU-004", "nombre": "Challapata Altiplano Sur",         "lat": -18.9000, "lon": -66.7667, "depto": "Oruro", "tecnologia": "Microondas", "frec_mhz": 700},
    {"torre_id": "ORU-005", "nombre": "Huanuni Complejo Minero",          "lat": -18.2833, "lon": -66.8333, "depto": "Oruro", "tecnologia": "Microondas", "frec_mhz": 1900},

    # ── POTOSÍ (5) ─────────────────────────────────────────────────────
    {"torre_id": "POT-001", "nombre": "Potosí Centro Histórico Cerro Rico","lat": -19.5836, "lon": -65.7531, "depto": "Potosí", "tecnologia": "4G LTE", "frec_mhz": 1900},
    {"torre_id": "POT-002", "nombre": "Potosí San Roque Minero",          "lat": -19.5750, "lon": -65.7650, "depto": "Potosí", "tecnologia": "4G LTE", "frec_mhz": 700},
    {"torre_id": "POT-003", "nombre": "Uyuni Salar Turístico",            "lat": -20.4631, "lon": -66.8250, "depto": "Potosí", "tecnologia": "4G LTE", "frec_mhz": 1900},
    {"torre_id": "POT-004", "nombre": "Llallagua Siglo XX",               "lat": -18.4239, "lon": -66.5839, "depto": "Potosí", "tecnologia": "Microondas", "frec_mhz": 700},
    {"torre_id": "POT-005", "nombre": "Tupiza Chichas",                   "lat": -21.4433, "lon": -65.7183, "depto": "Potosí", "tecnologia": "Microondas", "frec_mhz": 1900},

    # ── CHUQUISACA (5) ─────────────────────────────────────────────────
    {"torre_id": "CHQ-001", "nombre": "Sucre Plaza 25 de Mayo Capital",   "lat": -19.0478, "lon": -65.2594, "depto": "Chuquisaca", "tecnologia": "5G NR",  "frec_mhz": 3500},
    {"torre_id": "CHQ-002", "nombre": "Sucre Parque Bolívar",             "lat": -19.0400, "lon": -65.2630, "depto": "Chuquisaca", "tecnologia": "4G LTE", "frec_mhz": 2600},
    {"torre_id": "CHQ-003", "nombre": "Sucre Ciudad Universitaria USFX",  "lat": -19.0550, "lon": -65.2500, "depto": "Chuquisaca", "tecnologia": "4G LTE", "frec_mhz": 1900},
    {"torre_id": "CHQ-004", "nombre": "Monteagudo Chaco Chuquisaqueño",   "lat": -19.8000, "lon": -63.9556, "depto": "Chuquisaca", "tecnologia": "Microondas", "frec_mhz": 700},
    {"torre_id": "CHQ-005", "nombre": "Tarabuco Textil Originario",        "lat": -19.1833, "lon": -64.9167, "depto": "Chuquisaca", "tecnologia": "Microondas", "frec_mhz": 1900},

    # ── TARIJA (5) ─────────────────────────────────────────────────────
    {"torre_id": "TAR-001", "nombre": "Tarija Centro El Molino",          "lat": -21.5355, "lon": -64.7296, "depto": "Tarija", "tecnologia": "5G NR",  "frec_mhz": 3500},
    {"torre_id": "TAR-002", "nombre": "Tarija San Martín Mercado",        "lat": -21.5280, "lon": -64.7350, "depto": "Tarija", "tecnologia": "4G LTE", "frec_mhz": 1900},
    {"torre_id": "TAR-003", "nombre": "Yacuiba Gran Chaco Frontera",      "lat": -22.0164, "lon": -63.6778, "depto": "Tarija", "tecnologia": "4G LTE", "frec_mhz": 2600},
    {"torre_id": "TAR-004", "nombre": "Bermejo Frontera Argentina",       "lat": -22.7308, "lon": -64.3378, "depto": "Tarija", "tecnologia": "Microondas", "frec_mhz": 700},
    {"torre_id": "TAR-005", "nombre": "Villamontes Pilcomayo Gasífera",   "lat": -21.2589, "lon": -63.4500, "depto": "Tarija", "tecnologia": "4G LTE", "frec_mhz": 1900},

    # ── PANDO (4) ──────────────────────────────────────────────────────
    {"torre_id": "PAN-001", "nombre": "Cobija Centro Aduana Frontera",     "lat": -11.0267, "lon": -68.7692, "depto": "Pando", "tecnologia": "4G LTE", "frec_mhz": 1900},
    {"torre_id": "PAN-002", "nombre": "Cobija Puerto Alto Río Acre",      "lat": -11.0150, "lon": -68.7500, "depto": "Pando", "tecnologia": "4G LTE", "frec_mhz": 700},
    {"torre_id": "PAN-003", "nombre": "Porvenir Cruce Amazónico",         "lat": -11.2333, "lon": -68.6833, "depto": "Pando", "tecnologia": "Microondas", "frec_mhz": 700},
    {"torre_id": "PAN-004", "nombre": "Bella Flor Castañera",             "lat": -11.5167, "lon": -67.6833, "depto": "Pando", "tecnologia": "Microondas", "frec_mhz": 1900},
]

# ─────────────────────────────────────────────
# PARÁMETROS DE SIMULACIÓN Y TRANSMISIÓN
# ─────────────────────────────────────────────
OUTPUT_DIR   = "stream_input"
INTERVAL_SEC = 1.0         # 1 segundo entre emisiones (alta velocidad)
ANOMALY_PROB = 0.12        # 12% probabilidad de que surja una anomalía
ANOMALY_DUR  = 6           # ciclos que dura una falla activa

# Estado de anomalía por torre (0: normal, >0: ciclos restantes de falla)
anomaly_state = {t["torre_id"]: 0 for t in TORRES}
anomaly_type  = {t["torre_id"]: "" for t in TORRES}
batch_counter = 0

TIPOS_ANOMALIA = [
    ("CORTE_ENLACE",     "Corte de Enlace Backhaul / Fibra"),
    ("CONGESTION_ALTA",  "Congestión Masiva de Usuarios"),
    ("SOBRECALENTAMIENTO","Sobrecalentamiento de Transmisor"),
    ("ATENUACION_CLIMA", "Atenuación por Tormenta Tropical"),
]

def generar_telemetria_normal(torre: dict) -> dict:
    """Genera valores de telemetría nominales para una torre."""
    # Usuarios base según tecnología
    users_base = 650 if torre["tecnologia"] == "5G NR" else (350 if torre["tecnologia"] == "4G LTE" else 120)
    usuarios = int(random.gauss(users_base, users_base * 0.15))
    usuarios = max(20, min(1500, usuarios))

    # Tráfico proporcional a usuarios
    trafico = round(usuarios * random.uniform(0.8, 1.8), 2)
    latencia = round(random.uniform(15.0, 85.0), 2)
    perdida = round(random.uniform(0.0, 3.5), 2)
    jitter = round(random.uniform(1.0, 12.0), 2)
    temp = round(random.uniform(28.0, 48.0), 1)

    return {
        "torre_id":            torre["torre_id"],
        "nombre":              torre["nombre"],
        "lat":                 torre["lat"],
        "lon":                 torre["lon"],
        "depto":               torre["depto"],
        "tecnologia":          torre["tecnologia"],
        "frec_mhz":            torre["frec_mhz"],
        "latencia_ms":         latencia,
        "perdida_pct":         perdida,
        "trafico_mbps":        trafico,
        "jitter_ms":           jitter,
        "temperatura_c":       temp,
        "usuarios_conectados": usuarios,
        "senal_dbm":           round(random.uniform(-75.0, -52.0), 2),
        "cpu_pct":             round(random.uniform(20.0, 60.0), 2),
        "timestamp":           datetime.now(timezone.utc).isoformat(),
        "es_anomalia":         False,
        "tipo_falla":          "NINGUNA",
    }


def generar_telemetria_anomalia(torre: dict, tipo: str) -> dict:
    """Genera valores anómalos multidimensionales realistas según el tipo de incidente."""
    base = generar_telemetria_normal(torre)
    base["es_anomalia"] = True
    base["tipo_falla"] = tipo

    if tipo == "CORTE_ENLACE":
        base["latencia_ms"] = round(random.uniform(320.0, 680.0), 2)
        base["perdida_pct"] = round(random.uniform(45.0, 92.0), 2)
        base["jitter_ms"]   = round(random.uniform(40.0, 120.0), 2)
        base["trafico_mbps"]= round(base["trafico_mbps"] * 0.15, 2)  # se desploma el tráfico

    elif tipo == "CONGESTION_ALTA":
        base["usuarios_conectados"] = int(base["usuarios_conectados"] * random.uniform(2.2, 3.5))
        base["trafico_mbps"]        = round(random.uniform(1200.0, 2400.0), 2)
        base["latencia_ms"]         = round(random.uniform(140.0, 240.0), 2)
        base["perdida_pct"]         = round(random.uniform(12.0, 25.0), 2)
        base["cpu_pct"]             = round(random.uniform(85.0, 98.0), 2)

    elif tipo == "SOBRECALENTAMIENTO":
        base["temperatura_c"] = round(random.uniform(72.0, 94.0), 1)
        base["cpu_pct"]       = round(random.uniform(88.0, 100.0), 2)
        base["latencia_ms"]   = round(random.uniform(160.0, 310.0), 2)
        base["perdida_pct"]   = round(random.uniform(18.0, 42.0), 2)

    elif tipo == "ATENUACION_CLIMA":
        base["senal_dbm"]   = round(random.uniform(-105.0, -92.0), 2)  # señal degradada
        base["latencia_ms"] = round(random.uniform(130.0, 220.0), 2)
        base["perdida_pct"] = round(random.uniform(14.0, 32.0), 2)
        base["jitter_ms"]   = round(random.uniform(25.0, 60.0), 2)

    return base


def emitir_batch(batch_num: int) -> None:
    """Genera un archivo JSON (NDJSON) con la telemetría de las 62 torres."""
    registros = []
    global anomaly_state, anomaly_type

    # Forzar al menos 3 fallas en el primer ciclo para demostración inmediata
    if batch_num == 0:
        anomaly_state["BEN-002"] = ANOMALY_DUR  # Trinidad UABJB Campus
        anomaly_type["BEN-002"]  = "CONGESTION_ALTA"
        anomaly_state["LPZ-006"] = ANOMALY_DUR  # El Alto Ceja
        anomaly_type["LPZ-006"]  = "CORTE_ENLACE"
        anomaly_state["SCZ-001"] = ANOMALY_DUR  # SCZ Equipetrol
        anomaly_type["SCZ-001"]  = "SOBRECALENTAMIENTO"

    for torre in TORRES:
        tid = torre["torre_id"]

        # Si ya está en anomalía, decrementar contador
        if anomaly_state[tid] > 0:
            anomaly_state[tid] -= 1
            registro = generar_telemetria_anomalia(torre, anomaly_type[tid])
        else:
            # Posibilidad aleatoria de entrar en falla
            if random.random() < ANOMALY_PROB:
                tipo_sel, _ = random.choice(TIPOS_ANOMALIA)
                anomaly_state[tid] = ANOMALY_DUR
                anomaly_type[tid]  = tipo_sel
                registro = generar_telemetria_anomalia(torre, tipo_sel)
            else:
                registro = generar_telemetria_normal(torre)

        registros.append(registro)

    # Escritura atómica del batch en formato NDJSON (New-line Delimited JSON)
    nombre_archivo = f"telemetria_batch_{batch_num:08d}.json"
    ruta_tmp   = os.path.join(OUTPUT_DIR, f".tmp_{nombre_archivo}")
    ruta_final = os.path.join(OUTPUT_DIR, nombre_archivo)

    with open(ruta_tmp, "w", encoding="utf-8") as f:
        for r in registros:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")

    os.replace(ruta_tmp, ruta_final)

    # Resumen en consola
    criticos = sum(1 for r in registros if r["latencia_ms"] > 250 or r["perdida_pct"] > 30)
    alertas  = sum(1 for r in registros if (r["latencia_ms"] > 120 or r["perdida_pct"] > 10) and not (r["latencia_ms"] > 250 or r["perdida_pct"] > 30))
    optimos  = len(registros) - criticos - alertas

    print(f"[{datetime.now().strftime('%H:%M:%S')}] Batch #{batch_num:06d} (62 Torres) → "
          f"CRÍTICOS: {criticos} | ALERTAS: {alertas} | ÓPTIMOS: {optimos} | "
          f"Archivo: {nombre_archivo}")


# ─────────────────────────────────────────────
# BUCLE PRINCIPAL
# ─────────────────────────────────────────────
if __name__ == "__main__":
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    print("=" * 75)
    print("  SIMULADOR MASIVO DE TELEMETRÍA NOC — RED NACIONAL BOLIVIA")
    print("=" * 75)
    print(f"  Torres monitoreadas : {len(TORRES)} en 9 departamentos")
    print(f"  Beni / Trinidad     : 6 torres (incluye Campus UABJB)")
    print(f"  Intervalo de emisión: {INTERVAL_SEC} segundo(s)")
    print(f"  Directorio salida   : ./{OUTPUT_DIR}/")
    print("=" * 75)
    print("  Iniciando streaming masivo... (Ctrl+C para detener)\n")

    try:
        while True:
            emitir_batch(batch_counter)
            batch_counter += 1
            time.sleep(INTERVAL_SEC)
    except KeyboardInterrupt:
        print("\n\n  Simulador detenido.")
        print(f"  Total de batches emitidos: {batch_counter}")

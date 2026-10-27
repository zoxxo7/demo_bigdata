"""
app.py — Dashboard NOC Interactivo con Streamlit + Folium (Escala Masiva Bolivia - 62 Torres)
Visualiza en tiempo real el estado de la red nacional de telecomunicaciones en los 9 departamentos.
Se auto-refresca leyendo los micro-batches procesados por Apache Spark en stream_output/.
"""

import os
import glob
import time
import warnings
import pandas as pd
import streamlit as st
import folium
from streamlit_folium import st_folium
from datetime import datetime

warnings.filterwarnings("ignore")

# ─────────────────────────────────────────────
# CONFIGURACIÓN DE PÁGINA
# ─────────────────────────────────────────────
st.set_page_config(
    page_title="NOC Bolivia — Monitoreo Big Data en Tiempo Real",
    page_icon="📡",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Estado persistente para el mapa (zoom y posición fija en actualizaciones)
if "map_zoom" not in st.session_state:
    st.session_state.map_zoom = 6
if "map_center" not in st.session_state:
    st.session_state.map_center = [-16.2902, -63.5887]
if "prev_sel_depto" not in st.session_state:
    st.session_state.prev_sel_depto = "TODOS"

# ─────────────────────────────────────────────
# ESTILOS PERSONALIZADOS
# ─────────────────────────────────────────────
st.markdown("""
<style>
    .stApp { background-color: #0d1117; color: #e6edf3; }
    .block-container { padding-top: 3rem; padding-bottom: 2.5rem; }

    div[data-testid="metric-container"] {
        background: linear-gradient(135deg, #161b22 0%, #1a2230 100%);
        border: 1px solid #30363d;
        border-radius: 12px;
        padding: 14px 18px;
        box-shadow: 0 6px 20px rgba(0,0,0,0.45);
        transition: transform 0.2s ease, border-color 0.2s ease;
    }
    div[data-testid="metric-container"]:hover {
        border-color: #58a6ff;
        transform: translateY(-2px);
    }
    div[data-testid="metric-container"] label {
        color: #8b949e !important;
        font-size: 0.78rem !important;
        font-weight: 700;
        text-transform: uppercase;
        letter-spacing: 0.06em;
    }
    div[data-testid="metric-container"] div[data-testid="stMetricValue"] {
        font-size: 1.65rem !important;
        font-weight: 800 !important;
        color: #ffffff !important;
        letter-spacing: -0.02em;
    }

    .noc-header {
        background: linear-gradient(90deg, #161b22 0%, #1c2433 50%, #161b22 100%);
        border: 1px solid #30363d;
        border-left: 5px solid #238636;
        border-radius: 12px;
        padding: 16px 22px;
        margin-bottom: 20px;
        box-shadow: 0 4px 20px rgba(0,0,0,0.5);
    }
    .noc-title {
        font-size: 1.75rem;
        font-weight: 800;
        color: #ffffff;
        letter-spacing: 0.02em;
        line-height: 1.3;
        display: inline-block;
        vertical-align: middle;
    }
    .noc-subtitle {
        color: #58a6ff;
        font-size: 0.92rem;
        margin-top: 6px;
        line-height: 1.4;
    }
    .live-badge {
        display: inline-block;
        background: #da3633;
        color: #ffffff;
        font-size: 0.72rem;
        font-weight: 800;
        padding: 3px 10px;
        border-radius: 20px;
        animation: pulse 1.5s infinite;
        margin-left: 12px;
        vertical-align: middle;
        box-shadow: 0 0 12px rgba(218, 54, 51, 0.6);
    }
    @keyframes pulse {
        0%   { opacity: 1; transform: scale(1); }
        50%  { opacity: 0.5; transform: scale(0.97); }
        100% { opacity: 1; transform: scale(1); }
    }

    section[data-testid="stSidebar"] { background-color: #161b22; border-right: 1px solid #30363d; }
    div[data-testid="stDataFrame"] { border: 1px solid #30363d; border-radius: 10px; overflow: hidden; }
</style>
""", unsafe_allow_html=True)

# ─────────────────────────────────────────────
# RUTAS Y CATÁLOGO POR DEFECTO
# ─────────────────────────────────────────────
OUTPUT_DIR = "stream_output"

TORRES_DEFAULT = [
    {"torre_id": "BEN-001", "nombre": "Trinidad Centro", "depto": "Beni", "tecnologia": "4G LTE", "lat": -14.8347, "lon": -64.9042, "latencia_ms": 42.0, "perdida_pct": 1.1, "trafico_mbps": 480.0, "jitter_ms": 4.5, "temperatura_c": 34.0, "usuarios_conectados": 420, "estado": "ÓPTIMO", "diagnostico": "Operación Nominal", "score_salud": 94.0},
    {"torre_id": "BEN-002", "nombre": "Trinidad Campus UABJB", "depto": "Beni", "tecnologia": "5G NR", "lat": -14.8210, "lon": -64.8950, "latencia_ms": 190.0, "perdida_pct": 18.0, "trafico_mbps": 1650.0, "jitter_ms": 22.0, "temperatura_c": 42.0, "usuarios_conectados": 1280, "estado": "ALERTA", "diagnostico": "Congestión de Celdas (Demanda)", "score_salud": 61.0},
    {"torre_id": "SCZ-001", "nombre": "SCZ Equipetrol", "depto": "Santa Cruz", "tecnologia": "5G NR", "lat": -17.7689, "lon": -63.1932, "latencia_ms": 35.0, "perdida_pct": 0.8, "trafico_mbps": 980.0, "jitter_ms": 3.0, "temperatura_c": 32.0, "usuarios_conectados": 710, "estado": "ÓPTIMO", "diagnostico": "Operación Nominal", "score_salud": 96.0},
    {"torre_id": "LPZ-006", "nombre": "El Alto Ceja", "depto": "La Paz", "tecnologia": "4G LTE", "lat": -16.5100, "lon": -68.1600, "latencia_ms": 390.0, "perdida_pct": 52.0, "trafico_mbps": 45.0, "jitter_ms": 85.0, "temperatura_c": 30.0, "usuarios_conectados": 80, "estado": "CRÍTICO", "diagnostico": "Corte de Enlace / Fibra", "score_salud": 16.0},
    {"torre_id": "CBB-001", "nombre": "CBB Cala Cala", "depto": "Cochabamba", "tecnologia": "5G NR", "lat": -17.3750, "lon": -66.1600, "latencia_ms": 38.0, "perdida_pct": 1.0, "trafico_mbps": 820.0, "jitter_ms": 3.8, "temperatura_c": 31.0, "usuarios_conectados": 590, "estado": "ÓPTIMO", "diagnostico": "Operación Nominal", "score_salud": 95.0},
    {"torre_id": "CHQ-001", "nombre": "Sucre Plaza 25 de Mayo", "depto": "Chuquisaca", "tecnologia": "5G NR", "lat": -19.0478, "lon": -65.2594, "latencia_ms": 44.0, "perdida_pct": 1.4, "trafico_mbps": 650.0, "jitter_ms": 4.2, "temperatura_c": 29.0, "usuarios_conectados": 490, "estado": "ÓPTIMO", "diagnostico": "Operación Nominal", "score_salud": 93.0},
]

# ─────────────────────────────────────────────
# CARGA DE DATOS DESDE STREAM_OUTPUT/
# ─────────────────────────────────────────────
@st.cache_data(ttl=2)
def cargar_datos() -> tuple[pd.DataFrame, int]:
    """Lee todos los micro-batches CSV generados por Spark."""
    patron = os.path.join(OUTPUT_DIR, "*.csv")
    archivos = sorted(glob.glob(patron), key=os.path.getmtime, reverse=True)

    if not archivos:
        return pd.DataFrame(TORRES_DEFAULT), 0

    total_archivos = len(archivos)
    # Leer los 40 archivos más recientes para velocidad
    archivos_recientes = archivos[:40]

    partes = []
    for f in archivos_recientes:
        try:
            if os.path.getsize(f) > 0:
                df_p = pd.read_csv(f, dtype=str)
                if not df_p.empty and "torre_id" in df_p.columns:
                    partes.append(df_p)
        except Exception:
            continue

    if not partes:
        return pd.DataFrame(TORRES_DEFAULT), total_archivos

    df = pd.concat(partes, ignore_index=True)

    # Convertir columnas numéricas
    num_cols = ["lat", "lon", "latencia_ms", "perdida_pct", "trafico_mbps",
                "jitter_ms", "temperatura_c", "usuarios_conectados", "cpu_pct", "score_salud"]
    for c in num_cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    # Quedarse con la última lectura por torre
    if "procesado_ts" in df.columns:
        df["procesado_ts"] = pd.to_datetime(df["procesado_ts"], errors="coerce")
        df = df.sort_values("procesado_ts", ascending=False)
        df = df.groupby("torre_id").first().reset_index()

    # Eliminar filas donde torre_id sea nulo o vacío
    if "torre_id" in df.columns:
        df = df[df["torre_id"].notna() & (df["torre_id"] != "")]

    if df.empty:
        return pd.DataFrame(TORRES_DEFAULT), total_archivos

    if "estado" not in df.columns:
        df["estado"] = "ÓPTIMO"
    if "diagnostico" not in df.columns:
        df["diagnostico"] = "Operación Nominal"
    if "depto" not in df.columns:
        df["depto"] = "Nacional"

    return df, total_archivos


# ─────────────────────────────────────────────
# MAPA FOLIUM CON CAPA LIMPIA
# ─────────────────────────────────────────────
COLOR_ESTADO = {
    "CRÍTICO": "#ef4444",
    "ALERTA":  "#f59e0b",
    "ÓPTIMO":  "#22c55e",
}
ICON_ESTADO = {
    "CRÍTICO": "🔴",
    "ALERTA":  "🟡",
    "ÓPTIMO":  "🟢",
}

def construir_mapa(df: pd.DataFrame, location=None, zoom_start=6) -> folium.Map:
    loc = location if location is not None else [-16.2902, -63.5887]
    z = zoom_start if zoom_start is not None else 6

    mapa = folium.Map(
        location=loc,
        zoom_start=z,
        tiles=None,
        prefer_canvas=True,
    )

    # Capa 1: Esri Dark Gray (Modo oscuro sin watermark)
    folium.TileLayer(
        tiles="https://server.arcgisonline.com/ArcGIS/rest/services/Canvas/World_Dark_Gray_Base/MapServer/tile/{z}/{y}/{x}",
        attr="Esri",
        name="Modo Oscuro NOC",
        overlay=False,
        control=True,
    ).add_to(mapa)

    # Capa 2: OpenStreetMap tradicional
    folium.TileLayer(
        tiles="OpenStreetMap",
        name="OpenStreetMap Estándar",
        overlay=False,
        control=True,
    ).add_to(mapa)

    folium.LayerControl(position="topright").add_to(mapa)

    for _, row in df.iterrows():
        estado     = str(row.get("estado", "ÓPTIMO")).strip()
        color      = COLOR_ESTADO.get(estado, "#22c55e")
        icono      = ICON_ESTADO.get(estado, "🟢")
        lat        = float(row.get("lat", -16.5))
        lon        = float(row.get("lon", -64.8))
        nombre     = str(row.get("nombre", row.get("torre_id", "N/A")))
        tid        = str(row.get("torre_id", ""))
        depto      = str(row.get("depto", "Bolivia"))
        tec        = str(row.get("tecnologia", "4G"))
        latencia   = float(row.get("latencia_ms", 0))
        perdida    = float(row.get("perdida_pct", 0))
        trafico    = float(row.get("trafico_mbps", 0))
        temp       = float(row.get("temperatura_c", 30))
        users      = int(row.get("usuarios_conectados", 0))
        diag       = str(row.get("diagnostico", "Operación Nominal"))
        score      = float(row.get("score_salud", 0))

        radio = max(7, min(24, int(latencia / 14)))

        popup_html = f"""
        <div style="font-family:sans-serif; font-size:12px; min-width:240px;
                    background:#161b22; color:#f0f6fc; padding:12px; border-radius:8px;
                    border-left:4px solid {color}; box-shadow:0 4px 12px rgba(0,0,0,0.5);">
            <b style="font-size:14px;">{icono} {nombre}</b><br>
            <span style="color:#8b949e;">{tid} • {depto} • {tec}</span>
            <hr style="border-color:#30363d; margin:6px 0;">
            <b>Estado :</b> <span style="color:{color}; font-weight:700;">{estado}</span><br>
            <b>Causa  :</b> <span style="color:#fcd34d;">{diag}</span><br>
            <b>Latencia:</b> {latencia:.1f} ms  |  <b>Pérdida:</b> {perdida:.1f}%<br>
            <b>Tráfico :</b> {trafico:.0f} Mbps |  <b>Usuarios:</b> {users}<br>
            <b>Temp    :</b> {temp:.1f} °C   |  <b>Salud:</b> {score:.1f}/100
        </div>
        """

        # Círculo de cobertura
        folium.Circle(
            location=[lat, lon],
            radius=radio * 1200,
            color=color,
            fill=True,
            fill_opacity=0.07,
            weight=1,
            opacity=0.25,
        ).add_to(mapa)

        # Marcador
        folium.CircleMarker(
            location=[lat, lon],
            radius=radio,
            color=color,
            fill=True,
            fill_color=color,
            fill_opacity=0.88,
            weight=2,
            popup=folium.Popup(popup_html, max_width=280),
            tooltip=f"{icono} {nombre} ({depto}) — {estado} [{latencia:.0f}ms]",
        ).add_to(mapa)

    return mapa


# ─────────────────────────────────────────────
# CARGA Y FILTROS
# ─────────────────────────────────────────────
df_raw, total_archivos_spark = cargar_datos()

# SIDEBAR DE CONTROL
with st.sidebar:
    st.markdown("## ⚙️ Centro de Control NOC")
    st.markdown("---")

    refresh_rate = st.slider("🔄 Auto-refresco (segundos)", 1, 15, 3)
    st.markdown("---")

    st.markdown("### 🔍 Filtros de Red")
    deptos_disp = ["TODOS"] + sorted(list(df_raw["depto"].dropna().unique())) if "depto" in df_raw.columns else ["TODOS"]
    sel_depto = st.selectbox("📍 Departamento", deptos_disp, index=0)

    tecs_disp = ["TODAS"] + sorted(list(df_raw["tecnologia"].dropna().unique())) if "tecnologia" in df_raw.columns else ["TODAS"]
    sel_tec = st.selectbox("📶 Tecnología", tecs_disp, index=0)

    estados_disp = ["TODOS", "CRÍTICO", "ALERTA", "ÓPTIMO"]
    sel_estado = st.selectbox("🚨 Nivel de Severidad", estados_disp, index=0)

    st.markdown("---")
    st.markdown("### 📊 Dimensiones Big Data")
    st.markdown(f"""
    - **Velocidad**: Streaming cada 1s
    - **Volumen**: 62 torres simultáneas
    - **Variedad**: 17 métricas + Geo + RF
    - **Veracidad**: Diagnóstico Spark
    - **Valor**: Detección de fallas en vivo
    """)

# APLICAR FILTROS
if sel_depto != st.session_state.prev_sel_depto:
    st.session_state.prev_sel_depto = sel_depto
    if sel_depto != "TODOS" and not df_raw.empty:
        depto_df = df_raw[df_raw["depto"] == sel_depto]
        if not depto_df.empty:
            st.session_state.map_center = [float(depto_df["lat"].mean()), float(depto_df["lon"].mean())]
            st.session_state.map_zoom = 9 if sel_depto in ["Beni", "Pando"] else 10
    else:
        st.session_state.map_center = [-16.2902, -63.5887]
        st.session_state.map_zoom = 6

df = df_raw.copy()
if sel_depto != "TODOS":
    df = df[df["depto"] == sel_depto]
if sel_tec != "TODAS":
    df = df[df["tecnologia"] == sel_tec]
if sel_estado != "TODOS":
    df = df[df["estado"] == sel_estado]


# ─────────────────────────────────────────────
# HEADER PRINCIPAL
# ─────────────────────────────────────────────
st.markdown("""
<div class="noc-header">
    <span class="noc-title">📡 NOC BOLIVIA — CENTRO DE CONTROL NACIONAL</span>
    <span class="live-badge">● STREAMING EN VIVO</span><br>
    <span class="noc-subtitle">
        Procesamiento Distribuido en Tiempo Real con Apache Spark Structured Streaming • Red Nacional de 62 Torres
    </span>
</div>
""", unsafe_allow_html=True)

with st.expander("🎓 Arquitectura del Sistema & Fundamentos de Big Data (Panel para Defensa)", expanded=False):
    c_arch1, c_arch2 = st.columns([1, 1])
    with c_arch1:
        st.markdown("""
        #### 🏗️ Arquitectura del Pipeline Streaming
        * **1. Ingesta (`simulador.py`)**: Productor IoT que emite telemetría continua cada 1 segundo para 62 antenas en los 9 departamentos.
        * **2. Buffer de Ingesta (`stream_input/`)**: Almacenamiento desacoplado en formato NDJSON (New-line Delimited JSON).
        * **3. Motor Distribuido (`spark_processor.py`)**: Apache PySpark Structured Streaming procesando en paralelo sobre **24 núcleos de CPU**.
        * **4. Tolerancia a Fallos (`spark_checkpoint/`)**: Checkpointing de estado y Write-Ahead Logs (WAL) para garantizar entrega *exactly-once/at-least-once*.
        * **5. Capa de Presentación (`app.py`)**: Dashboard NOC de respuesta sub-segundo con mapas geoespaciales interactivos.
        """)
    with c_arch2:
        st.markdown("""
        #### 💎 Cumplimiento de las 5 V's de Big Data
        * **🚀 Velocidad**: Ingesta cada 1s y procesamiento en micro-batches continuos con baja latencia.
        * **📦 Volumen**: 62 nodos emitiendo concurrentemente más de 220,000 métricas/hora sin colapsar memoria RAM.
        * **🔀 Variedad**: JSON semiestructurado, capas de red (4G/5G/Microondas), telemetría RF y datos geoespaciales.
        * **🛡️ Veracidad**: Validación de esquema estricto (`StructType`), filtrado de lecturas corruptas y diagnóstico de causa raíz.
        * **💡 Valor**: Detección proactiva de incidentes (cortes de fibra, sobrecalentamiento, congestión) antes de la caída del servicio.
        """)

# ─────────────────────────────────────────────
# TARJETAS DE KPIS PRINCIPALES
# ─────────────────────────────────────────────
total_torres = len(df)
criticas     = int((df["estado"] == "CRÍTICO").sum()) if "estado" in df.columns else 0
alertas      = int((df["estado"] == "ALERTA").sum())  if "estado" in df.columns else 0
optimas      = int((df["estado"] == "ÓPTIMO").sum())  if "estado" in df.columns else 0
lat_prom     = round(df["latencia_ms"].mean(), 1) if "latencia_ms" in df.columns and not df.empty else 0.0
perd_prom    = round(df["perdida_pct"].mean(), 1) if "perdida_pct" in df.columns and not df.empty else 0.0
trafico_tot  = round(df["trafico_mbps"].sum(), 0) if "trafico_mbps" in df.columns and not df.empty else 0.0
users_tot    = int(df["usuarios_conectados"].sum()) if "usuarios_conectados" in df.columns and not df.empty else 0

c1, c2, c3, c4, c5, c6 = st.columns(6)
c1.metric("🗼 Torres Activas", f"{total_torres} / {len(df_raw)}")
c2.metric("🔴 Torres Críticas", f"{criticas}", delta=f"-{criticas}" if criticas > 0 else "0", delta_color="inverse")
c3.metric("🟡 En Alerta", f"{alertas}", delta=f"-{alertas}" if alertas > 0 else "0", delta_color="inverse")
c4.metric("⏱️ Latencia Media", f"{lat_prom} ms", delta=f"+{lat_prom-60:.1f}" if lat_prom > 60 else "Nominal", delta_color="inverse")
c5.metric("👥 Usuarios Celulares", f"{users_tot:,}")
c6.metric("📶 Tráfico Agregado", f"{trafico_tot/1000:.2f} Gbps")

st.markdown("<br>", unsafe_allow_html=True)

# ─────────────────────────────────────────────
# LAYOUT: MAPA + TABLA PRIORIZADA
# ─────────────────────────────────────────────
col_mapa, col_tabla = st.columns([11, 9], gap="medium")

with col_mapa:
    c_m1, c_m2 = st.columns([7, 3])
    with c_m1:
        st.markdown(f"### 🗺️ Mapa Geoespacial ({total_torres} Torres)")
    with c_m2:
        if st.button("🇧🇴 Vista Nacional", key="btn_reset_zoom", help="Restablecer zoom y centro a todo el país"):
            st.session_state.map_zoom = 6
            st.session_state.map_center = [-16.2902, -63.5887]
            st.rerun()

    mapa = construir_mapa(
        df,
        location=st.session_state.map_center,
        zoom_start=st.session_state.map_zoom,
    )
    map_state = st_folium(
        mapa,
        key="noc_bolivia_map",
        use_container_width=True,
        height=520,
        returned_objects=["zoom", "center"],
        zoom=st.session_state.map_zoom,
        center=tuple(st.session_state.map_center),
    )

    # Si el usuario hizo zoom o movió el mapa, retenerlo para futuros auto-refrescos
    if map_state:
        if map_state.get("zoom") is not None:
            st.session_state.map_zoom = int(map_state["zoom"])
        if map_state.get("center") is not None and isinstance(map_state["center"], dict):
            c_dict = map_state["center"]
            lat = c_dict.get("lat")
            lng = c_dict.get("lng")
            if lat is not None and lng is not None:
                st.session_state.map_center = [float(lat), float(lng)]

with col_tabla:
    st.markdown("### 📋 Telemetría y Diagnóstico de Causa Raíz")

    cols_ver = ["torre_id", "nombre", "depto", "estado", "diagnostico",
                "latencia_ms", "perdida_pct", "usuarios_conectados", "temperatura_c"]
    cols_ok = [c for c in cols_ver if c in df.columns]
    df_t = df[cols_ok].copy()

    renames = {
        "torre_id": "ID", "nombre": "Nombre Torre", "depto": "Depto",
        "estado": "Estado", "diagnostico": "Causa Estimada",
        "latencia_ms": "Lat (ms)", "perdida_pct": "Pérd (%)",
        "usuarios_conectados": "Usuarios", "temperatura_c": "Temp (°C)"
    }
    df_t.rename(columns=renames, inplace=True)

    # Ordenar críticos primero
    if "Estado" in df_t.columns:
        orden = {"CRÍTICO": 0, "ALERTA": 1, "ÓPTIMO": 2}
        df_t["_rank"] = df_t["Estado"].map(orden).fillna(3)
        df_t = df_t.sort_values("_rank").drop(columns=["_rank"])

    st.dataframe(df_t, use_container_width=True, height=520, hide_index=True)

st.markdown("---")

# ─────────────────────────────────────────────
# ANÁLISIS BIG DATA: DEPARTAMENTAL Y DIAGNÓSTICOS
# ─────────────────────────────────────────────
st.markdown("### 📊 Inteligencia Operacional Big Data")
col_b1, col_b2 = st.columns([1, 1], gap="medium")

with col_b1:
    st.markdown("#### 🏢 Estado de Red por Departamento")
    if "depto" in df_raw.columns and "estado" in df_raw.columns and not df_raw.empty:
        resumen_depto = pd.crosstab(df_raw["depto"], df_raw["estado"])
        st.bar_chart(resumen_depto)

with col_b2:
    st.markdown("#### ⚠️ Incidentes Críticos y Diagnóstico Inmediato")
    criticos_df = df_raw[df_raw["estado"].isin(["CRÍTICO", "ALERTA"])]
    if not criticos_df.empty:
        for _, inc in criticos_df.head(4).iterrows():
            badge = "🔴" if inc["estado"] == "CRÍTICO" else "🟡"
            st.error(f"{badge} **{inc['torre_id']} — {inc['nombre']}** ({inc.get('depto','')}) | "
                     f"**Causa:** {inc.get('diagnostico','N/A')} | "
                     f"Lat: {inc.get('latencia_ms',0):.0f}ms | Pérd: {inc.get('perdida_pct',0):.1f}% | Temp: {inc.get('temperatura_c',0):.0f}°C")
    else:
        st.success("✅ Todas las torres operan dentro de los umbrales nominales (SLA 99.9%).")

st.markdown("---")
col_f1, col_f2 = st.columns(2)
col_f1.caption(f"⚡ Motor de Procesamiento: **Apache PySpark 4.2.1** | Micro-batches procesados: **{total_archivos_spark:,}**")
col_f2.caption(f"🕒 Sincronización en vivo: **{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}** | Refresco: cada {refresh_rate}s")

# AUTO-REFRESCO
time.sleep(refresh_rate)
st.rerun()

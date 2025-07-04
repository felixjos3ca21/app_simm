
import streamlit as st
import pandas as pd
from pathlib import Path
from src.database.postgres import DatabaseManager
from src.utils.fondo import set_background
from src.procesador_streamlit.explorador import obtener_txt_recursivamente
from src.procesador_streamlit.clasificadores import detectar_tipo_archivo, calcular_hash_archivo, consultar_estado_archivo
from src.procesador_streamlit.procesador import procesar_archivo, clasificar_registros
from src.procesador_streamlit.consolidado import guardar_en_consolidado
from src.procesador_streamlit.insertador import insertar_registros, actualizar_registros, registrar_archivo

# ==============================================================================
# CONFIGURACIÓN STREAMLIT Y CONEXIÓN
# ==============================================================================
st.set_page_config(page_title="SIAMM - Actualización de Pagos", layout="wide")
st.image("src/utils/logo-andesbpo-359x143.png", width=150)
set_background("src/utils/bg-seccion.png")

engine_simm = DatabaseManager.get_engine("SIMM")

# ==============================================================================
# ESTADO DE SESIÓN
# ==============================================================================
if "archivos_validos" not in st.session_state:
    st.session_state["archivos_validos"] = []
if "resumen_exploracion" not in st.session_state:
    st.session_state["resumen_exploracion"] = {}
if "procesamiento_completo" not in st.session_state:
    st.session_state["procesamiento_completo"] = False
if "dataframes_para_bd" not in st.session_state:
    st.session_state["dataframes_para_bd"] = []
if "resumen_procesamiento" not in st.session_state:
    st.session_state["resumen_procesamiento"] = {}

# ==============================================================================
# SELECCIÓN DE CARPETA
# ==============================================================================
st.subheader("📂 Selección de Carpeta de Pagos")
carpeta = st.text_input("📁 Ruta de carpeta base:", value="", placeholder="Ej: C:/bases/pagos")
if not carpeta:
    st.stop()

# ==============================================================================
# BOTÓN 1 - EXPLORACIÓN DE ARCHIVOS
# ==============================================================================
if st.button("🔍 Explorar archivos"):
    archivos_txt = obtener_txt_recursivamente(Path(carpeta))
    encontrados = len(archivos_txt)

    archivos_validos = []
    archivos_invalidos = []
    for archivo in archivos_txt:
        tipo = detectar_tipo_archivo(archivo)
        if tipo in ["AP", "COMP"]:
            archivos_validos.append((archivo, tipo))
        else:
            archivos_invalidos.append(archivo)

    st.session_state["archivos_validos"] = archivos_validos
    st.session_state["resumen_exploracion"] = {
        "total": encontrados,
        "validos": len(archivos_validos),
        "invalidos": len(archivos_invalidos),
        "nombres_invalidos": [a.name for a in archivos_invalidos]
    }
    st.session_state["procesamiento_completo"] = False
    st.session_state["dataframes_para_bd"] = []
    st.success("Exploración completada ✅")

# Mostrar resumen de exploración
resumen_exploracion = st.session_state["resumen_exploracion"]
if resumen_exploracion:
    st.subheader("📋 Resumen de exploración")
    st.write(f"Total archivos encontrados: {resumen_exploracion['total']}")
    st.write(f"Archivos válidos (AP/COMP): {resumen_exploracion['validos']}")
    st.write(f"Archivos inválidos (nombre desconocido): {resumen_exploracion['invalidos']}")
    if resumen_exploracion["nombres_invalidos"]:
        with st.expander("📄 Ver nombres inválidos"):
            st.write(resumen_exploracion["nombres_invalidos"])

# ==============================================================================
# BOTÓN 2 - PROCESAR REGISTROS
# ==============================================================================
if st.button("⚙️ Procesar registros"):
    archivos = st.session_state["archivos_validos"]
    resumen = {"AP": [], "COMP": []}
    df_para_bd = []

    for archivo, tipo in archivos:
        df = procesar_archivo(archivo, tipo)
        guardar_en_consolidado(df, tipo, archivo.name)

        df = df.drop_duplicates(subset="id_registro")
        df = df[(df["valor"] > 0) & df["fecha_liquida"].notna()]
        if df.empty:
            continue

        tabla = "pagos_ap" if tipo == "AP" else "pagos_comparendos"
        df_nuevos, df_actualizables, df_duplicados = clasificar_registros(df, engine_simm, tabla)

        resumen[tipo].append({
            "archivo": archivo.name,
            "nuevos": len(df_nuevos),
            "actualizables": len(df_actualizables),
            "duplicados": len(df_duplicados),
            "valor_total": df["valor"].sum()
        })

        if not df_nuevos.empty:
            df_para_bd.append(("insert", df_nuevos, tabla, archivo.name, tipo))
        if not df_actualizables.empty:
            df_para_bd.append(("update", df_actualizables, tabla, archivo.name, tipo))

    st.session_state["dataframes_para_bd"] = df_para_bd
    st.session_state["resumen_procesamiento"] = resumen
    st.session_state["procesamiento_completo"] = True
    st.success("Procesamiento completado ✅")

# ==============================================================================
# RESUMEN DESPUÉS DEL PROCESAMIENTO
# ==============================================================================
if st.session_state["procesamiento_completo"]:
    st.subheader("📊 Resumen de registros listos para carga")
    resumen = st.session_state["resumen_procesamiento"]
    for tipo, archivos in resumen.items():
        if not archivos:
            continue
        st.markdown(f"### 📄 Tipo: {tipo}")
        for item in archivos:
            st.markdown(
                f"- 📂 {item['archivo']} | 🆕 Nuevos: {item['nuevos']} | ♻️ Actualizables: {item['actualizables']} | 🚫 Duplicados: {item['duplicados']} | 💰 Valor: ${item['valor_total']:,.2f}"
            )

# ==============================================================================
# BOTÓN 3 - CARGA A BASE DE DATOS
# ==============================================================================
if st.session_state["procesamiento_completo"] and st.button("💾 Cargar registros a la base de datos"):
    total_insert = 0
    total_update = 0
    for accion, df, tabla, archivo_name, tipo in st.session_state["dataframes_para_bd"]:
        if accion == "insert":
            total_insert += insertar_registros(df, tabla, engine_simm)
        elif accion == "update":
            total_update += actualizar_registros(df, tabla, engine_simm)

        registrar_archivo(
            nombre=archivo_name,
            tipo=tipo,
            cantidad=len(df),
            estado="exitoso",
            hash_archivo=calcular_hash_archivo(Path(archivo_name)),
            engine=engine_simm
        )

    st.success(f"✅ Registros cargados correctamente → Insertados: {total_insert}, Actualizados: {total_update}")

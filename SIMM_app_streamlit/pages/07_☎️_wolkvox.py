
import streamlit as st
import os
from pathlib import Path
from tkinter import Tk, filedialog
from datetime import datetime
from dotenv import load_dotenv
from src.utils.data_ingestor_tip3 import DataIngestorTip3
from src.utils.fondo import set_background
from src.utils.data_ingestor_camp_3 import DataIngestorCamp3

# Configuración de página
st.set_page_config(
    page_title="SIAMM - Carga Wolkvox",
    page_icon="src/utils/favicon-114x114.png",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Estilos CSS
st.markdown("""
    <style>
    [data-testid=stSidebar] { background: #a5d6a7 !important; padding: 20px 10px; }
    .main-container { padding: 2rem; background: linear-gradient(135deg, #f5f7fa 0%, #c3cfe2 100%); min-height: 100vh; }
    .metric-card { background: white; padding: 20px; border-radius: 15px; box-shadow: 0 8px 32px rgba(0,0,0,0.1); margin: 10px 0; transition: transform 0.3s ease; }
    .metric-card:hover { transform: translateY(-5px); }
    .section-title { background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); -webkit-background-clip: text; -webkit-text-fill-color: transparent; font-size: 2rem; margin: 30px 0 20px 0; text-align: center; }
    .section-header { color: #2e7d32; border-bottom: 2px solid #a5d6a7; padding-bottom: 0.5rem; margin-bottom: 1rem; }
    .stButton > button { background: linear-gradient(120deg, #a5d6a7 0%, #2ecc71 100%); color: white; border: none; border-radius: 25px; padding: 12px 30px; transition: all 0.3s ease; }
    .stButton > button:hover { transform: translateY(-2px); box-shadow: 0 6px 20px rgba(102, 126, 234, 0.6); }
    .stProgress > div > div > div > div { background-color: #2e7d32; }
    .sidebar-title { color: #2c3e50; font-size: 1.2rem; margin-bottom: 1rem; font-weight: 600; }
    .success-box { background: #d4edda; border: 1px solid #c3e6cb; border-radius: 8px; padding: 15px; margin: 10px 0; }
    .error-box { background: #f8d7da; border: 1px solid #f5c6cb; border-radius: 8px; padding: 15px; margin: 10px 0; }
    .info-box { background: #d1ecf1; border: 1px solid #b3d4d8; border-radius: 8px; padding: 15px; margin: 10px 0; }
    .file-status-new { color: #28a745; font-weight: bold; }
    .file-status-modified { color: #ffc107; font-weight: bold; }
    .file-status-existing { color: #6c757d; }
    .directory-input { background: white; padding: 15px; border-radius: 10px; margin-bottom: 20px; }
    </style>
""", unsafe_allow_html=True)

# Logo y fondo
st.image("src/utils/logo-andesbpo-359x143.png", width=150)
set_background("src/utils/bg-seccion.png")

# Cargar variables de entorno
load_dotenv()

# Configuración de base de datos desde .env
DB_CONFIG = {
    'host': os.getenv('DB_SIMM_HOST', 'localhost'),
    'database': os.getenv('DB_SIMM_NAME', 'SIMM'),
    'user': os.getenv('DB_SIMM_USER', 'postgres'),
    'password': os.getenv('DB_SIMM_PASSWORD', 'admin'),
    'port': os.getenv('DB_SIMM_PORT', '5432')
}

def select_folder():
    """Abre explorador para seleccionar carpeta"""
    root = Tk()
    root.withdraw()
    root.wm_attributes('-topmost', 1)
    
    folder_path = filedialog.askdirectory(
        title="Seleccionar carpeta con archivos CSV"
    )
    
    root.destroy()
    return folder_path if folder_path else None

def show_tipificacion_3_ui():
    """Muestra la UI para Tipificación 3"""
    st.markdown("<h1 class='section-title'>🗂️ SIAMM - Carga Wolkvox Tipificación 3</h1>", unsafe_allow_html=True)
    
    # Inicializar procesador
    if 'processor_tip3' not in st.session_state:
        with st.spinner("Inicializando conexión a base de datos..."):
            st.session_state.processor_tip3 = DataIngestorTip3(DB_CONFIG)

    processor = st.session_state.processor_tip3

    # Verificar conexión
    if not processor.connection_status:
        st.markdown(f"""
        <div class='error-box'>
            <h3>❌ Error de Conexión</h3>
            <p>No se pudo conectar a la base de datos:</p>
            <code>{processor.error_message}</code>
            <p><strong>Verifica:</strong></p>
            <ul>
                <li>Que PostgreSQL esté ejecutándose</li>
                <li>Las credenciales en el archivo .env</li>
                <li>La conectividad de red</li>
            </ul>
        </div>
        """, unsafe_allow_html=True)
        return

    # Conexión exitosa
    st.markdown("""
    <div class='success-box'>
        <h3>✅ Conexión Establecida</h3>
        <p>Sistema listo para procesar archivos CSV de Wolkvox</p>
    </div>
    """, unsafe_allow_html=True)

    # Selección de carpeta
    st.markdown("<h2 class='section-header'>📂 Selección de Carpeta</h2>", unsafe_allow_html=True)
    
    col1, col2 = st.columns([3, 1])
    with col1:
        if 'selected_folder_tip3' not in st.session_state:
            st.session_state.selected_folder_tip3 = None
        
        if st.session_state.selected_folder_tip3:
            st.info(f"📁 Carpeta seleccionada: `{st.session_state.selected_folder_tip3}`")
        else:
            st.warning("⚠️ No se ha seleccionado ninguna carpeta")
    
    with col2:
        if st.button("🗂️ Seleccionar Carpeta", type="primary", key="select_folder_tip3"):
            folder_path = select_folder()
            if folder_path:
                st.session_state.selected_folder_tip3 = folder_path
                st.rerun()

    # Análisis de archivos
    if st.session_state.selected_folder_tip3:
        st.markdown("<h2 class='section-header'>📋 Análisis de Archivos</h2>", unsafe_allow_html=True)
        
        with st.spinner("Analizando archivos..."):
            files = processor.get_files_to_process(st.session_state.selected_folder_tip3)
        
        # Estadísticas
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric("📄 Archivos Nuevos", len(files['new']), delta=None)
        
        with col2:
            st.metric("❌ Archivos Fallidos", len(files['failed']), delta=None)
        
        with col3:
            st.metric("✅ Ya Procesados", len(files['processed']), delta=None)
        
        with col4:
            total = len(files['new']) + len(files['failed'])
            st.metric("🎯 Total a Procesar", total, delta=None)

        # Detalles de archivos
        if files['new'] or files['failed'] or files['processed']:
            
            # Pestañas para organizar información
            tab1, tab2, tab3 = st.tabs(["📄 Nuevos", "❌ Fallidos", "✅ Procesados"])
            
            with tab1:
                if files['new']:
                    st.markdown("**Archivos nuevos encontrados:**")
                    for i, file in enumerate(files['new'], 1):
                        st.markdown(f"<span class='file-status-new'>{i}. {file}</span>", unsafe_allow_html=True)
                else:
                    st.info("No hay archivos nuevos para procesar")
            
            with tab2:
                if files['failed']:
                    st.markdown("**Archivos que fallaron previamente:**")
                    for i, file in enumerate(files['failed'], 1):
                        st.markdown(f"<span class='file-status-failed'>{i}. {file}</span>", unsafe_allow_html=True)
                else:
                    st.info("No hay archivos fallidos")
            
            with tab3:
                if files['processed']:
                    st.markdown("**Archivos ya procesados exitosamente:**")
                    for i, file in enumerate(files['processed'], 1):
                        st.markdown(f"<span class='file-status-processed'>{i}. {file}</span>", unsafe_allow_html=True)
                else:
                    st.info("No hay archivos procesados previamente")

        # Botón de procesamiento
        total_to_process = len(files['new']) + len(files['failed'])
        
        if total_to_process > 0:
            st.markdown("<h2 class='section-header'>🚀 Procesamiento</h2>", unsafe_allow_html=True)
            
            if st.button(f"🎯 Procesar {total_to_process} Archivos", type="primary", use_container_width=True, key="process_tip3"):
                
                # Contenedor para el progreso
                progress_container = st.container()
                log_container = st.container()
                
                with progress_container:
                    # Barra de progreso general
                    progress_bar = st.progress(0)
                    status_text = st.empty()
                    
                    # Métricas en tiempo real
                    col1, col2, col3 = st.columns(3)
                    with col1:
                        success_metric = st.empty()
                    with col2:
                        error_metric = st.empty()
                    with col3:
                        current_file = st.empty()

                with log_container:
                    st.markdown("### 📝 Log de Procesamiento")
                    log_placeholder = st.empty()
                    log_messages = []

                # Procesar archivos
                successful = 0
                failed = 0
                total_processed = 0
                
                # Procesar archivos nuevos
                for i, filename in enumerate(files['new']):
                    total_processed += 1
                    progress = total_processed / total_to_process
                    progress_bar.progress(progress)
                    status_text.text(f"Procesando archivo nuevo {i+1}/{len(files['new'])}: {filename}")
                    current_file.metric("📁 Archivo Actual", filename)
                    
                    log_messages.append(f"[{datetime.now().strftime('%H:%M:%S')}] Procesando: {filename}")
                    log_placeholder.markdown(
                        f"<div class='log-container'>{'<br>'.join(log_messages[-10:])}</div>", 
                        unsafe_allow_html=True
                    )
                    
                    result = processor._process_single_file(
                        os.path.join(st.session_state.selected_folder_tip3, filename)
                    )
                    
                    if result['success']:
                        successful += 1
                        log_messages.append(f"[{datetime.now().strftime('%H:%M:%S')}] ✅ Éxito: {result['records']} registros")
                    else:
                        failed += 1
                        log_messages.append(f"[{datetime.now().strftime('%H:%M:%S')}] ❌ Error: {result['error'][:100]}...")
                    
                    success_metric.metric("✅ Exitosos", successful)
                    error_metric.metric("❌ Fallidos", failed)
                
                # Procesar archivos fallidos (reintentos)
                for i, filename in enumerate(files['failed']):
                    total_processed += 1
                    progress = total_processed / total_to_process
                    progress_bar.progress(progress)
                    status_text.text(f"Reintentando archivo {i+1}/{len(files['failed'])}: {filename}")
                    current_file.metric("🔄 Reintentando", filename)
                    
                    log_messages.append(f"[{datetime.now().strftime('%H:%M:%S')}] Reintentando: {filename}")
                    log_placeholder.markdown(
                        f"<div class='log-container'>{'<br>'.join(log_messages[-10:])}</div>", 
                        unsafe_allow_html=True
                    )
                    
                    result = processor._process_single_file(
                        os.path.join(st.session_state.selected_folder_tip3, filename),
                        is_retry=True
                    )
                    
                    if result['success']:
                        successful += 1
                        log_messages.append(f"[{datetime.now().strftime('%H:%M:%S')}] ✅ Recuperado: {result['records']} registros")
                    else:
                        failed += 1
                        log_messages.append(f"[{datetime.now().strftime('%H:%M:%S')}] ❌ Falló nuevamente: {result['error'][:100]}...")
                    
                    success_metric.metric("✅ Exitosos", successful)
                    error_metric.metric("❌ Fallidos", failed)

                # Finalizar procesamiento
                progress_bar.progress(1.0)
                status_text.text("🎉 Procesamiento completado")
                current_file.empty()
                
                # Mostrar resumen final
                if successful == total_to_process:
                    st.markdown(f"""
                    <div class='success-box'>
                        <h3>🎉 Procesamiento Completado Exitosamente</h3>
                        <p><strong>Todos los archivos fueron procesados correctamente:</strong></p>
                        <ul>
                            <li>✅ Archivos exitosos: {successful}</li>
                            <li>📊 Total de archivos: {total_to_process}</li>
                        </ul>
                    </div>
                    """, unsafe_allow_html=True)
                elif successful > 0:
                    st.markdown(f"""
                    <div class='warning-box'>
                        <h3>⚠️ Procesamiento Completado con Advertencias</h3>
                        <p><strong>Resumen del procesamiento:</strong></p>
                        <ul>
                            <li>✅ Archivos exitosos: {successful}</li>
                            <li>❌ Archivos fallidos: {failed}</li>
                            <li>📊 Total procesados: {total_to_process}</li>
                        </ul>
                        <p><strong>Recomendación:</strong> Revisa los logs para identificar los errores en los archivos fallidos.</p>
                    </div>
                    """, unsafe_allow_html=True)
                else:
                    st.markdown(f"""
                    <div class='error-box'>
                        <h3>❌ Procesamiento Fallido</h3>
                        <p><strong>Ningún archivo pudo ser procesado exitosamente:</strong></p>
                        <ul>
                            <li>❌ Archivos fallidos: {failed}</li>
                            <li>📊 Total intentados: {total_to_process}</li>
                        </ul>
                        <p><strong>Sugerencias:</strong></p>
                        <ul>
                            <li>Verifica el formato de los archivos CSV</li>
                            <li>Revisa los logs de error para más detalles</li>
                            <li>Confirma que las columnas requeridas estén presentes</li>
                        </ul>
                    </div>
                    """, unsafe_allow_html=True)
                
                # Botón para limpiar y reiniciar
                if st.button("🔄 Procesar Nuevos Archivos", type="secondary", key="retry_tip3"):
                    st.rerun()
        
        elif total_to_process == 0 and files['processed']:
            st.markdown("""
            <div class='info-box'>
                <h3>ℹ️ Todos los Archivos Ya Están Procesados</h3>
                <p>Todos los archivos CSV en la carpeta seleccionada ya han sido procesados exitosamente.</p>
                <p><strong>Opciones:</strong></p>
                <ul>
                    <li>Selecciona una carpeta diferente con archivos nuevos</li>
                    <li>Agrega nuevos archivos CSV a la carpeta actual y actualiza</li>
                </ul>
            </div>
            """, unsafe_allow_html=True)
        
        else:
            st.markdown("""
            <div class='info-box'>
                <h3>📂 Carpeta Vacía</h3>
                <p>No se encontraron archivos CSV en la carpeta seleccionada.</p>
                <p><strong>Verifica que:</strong></p>
                <ul>
                    <li>La carpeta contenga archivos con extensión .csv</li>
                    <li>Los archivos no estén en subcarpetas</li>
                    <li>Tengas permisos de lectura en la carpeta</li>
                </ul>
            </div>
            """, unsafe_allow_html=True)

def show_cdr_ui():
    """Muestra la UI para CDR"""
    st.markdown("<h1 class='section-title'>📞 CDR - Próximamente</h1>", unsafe_allow_html=True)
    st.info("La funcionalidad para procesar CDR estará disponible en futuras actualizaciones.")

def show_campana_ui():
    """Muestra la UI para Campaña 3"""
    st.markdown("<h1 class='section-title'>📢 Campaña 3 - Procesamiento</h1>", unsafe_allow_html=True)
    
    # Inicializar procesador
    if 'processor_camp3' not in st.session_state:
        with st.spinner("Inicializando conexión a base de datos..."):
            st.session_state.processor_camp3 = DataIngestorCamp3(DB_CONFIG)

    processor = st.session_state.processor_camp3

    # Verificar conexión
    if not processor.connection_status:
        st.markdown(f"""
        <div class='error-box'>
            <h3>❌ Error de Conexión</h3>
            <p>No se pudo conectar a la base de datos:</p>
            <code>{processor.error_message}</code>
            <p><strong>Verifica:</strong></p>
            <ul>
                <li>Que PostgreSQL esté ejecutándose</li>
                <li>Las credenciales en el archivo .env</li>
                <li>La conectividad de red</li>
            </ul>
        </div>
        """, unsafe_allow_html=True)
        return

    # Conexión exitosa
    st.markdown("""
    <div class='success-box'>
        <h3>✅ Conexión Establecida</h3>
        <p>Sistema listo para procesar archivos CSV de Campaña 3</p>
    </div>
    """, unsafe_allow_html=True)

    # Selección de carpeta
    st.markdown("<h2 class='section-header'>📂 Selección de Carpeta</h2>", unsafe_allow_html=True)
    
    col1, col2 = st.columns([3, 1])
    with col1:
        if 'selected_folder_camp3' not in st.session_state:
            st.session_state.selected_folder_camp3 = None
        
        if st.session_state.selected_folder_camp3:
            st.info(f"📁 Carpeta seleccionada: `{st.session_state.selected_folder_camp3}`")
        else:
            st.warning("⚠️ No se ha seleccionado ninguna carpeta")
    
    with col2:
        if st.button("🗂️ Seleccionar Carpeta", type="primary", key="select_folder_camp3"):
            folder_path = select_folder()
            if folder_path:
                st.session_state.selected_folder_camp3 = folder_path
                st.rerun()

    # Análisis de archivos
    if st.session_state.selected_folder_camp3:
        st.markdown("<h2 class='section-header'>📋 Análisis de Archivos</h2>", unsafe_allow_html=True)
        
        with st.spinner("Analizando archivos..."):
            files = processor.get_files_to_process(st.session_state.selected_folder_camp3)
        
        # Estadísticas
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric("📄 Archivos Nuevos", len(files['new']), delta=None)
        
        with col2:
            st.metric("❌ Archivos Fallidos", len(files['failed']), delta=None)
        
        with col3:
            st.metric("✅ Ya Procesados", len(files['processed']), delta=None)
        
        with col4:
            total = len(files['new']) + len(files['failed'])
            st.metric("🎯 Total a Procesar", total, delta=None)

        # Detalles de archivos
        if files['new'] or files['failed'] or files['processed']:
            
            # Pestañas para organizar información
            tab1, tab2, tab3 = st.tabs(["📄 Nuevos", "❌ Fallidos", "✅ Procesados"])
            
            with tab1:
                if files['new']:
                    st.markdown("**Archivos nuevos encontrados:**")
                    for i, file in enumerate(files['new'], 1):
                        st.markdown(f"<span class='file-status-new'>{i}. {file}</span>", unsafe_allow_html=True)
                else:
                    st.info("No hay archivos nuevos para procesar")
            
            with tab2:
                if files['failed']:
                    st.markdown("**Archivos que fallaron previamente:**")
                    for i, file in enumerate(files['failed'], 1):
                        st.markdown(f"<span class='file-status-failed'>{i}. {file}</span>", unsafe_allow_html=True)
                else:
                    st.info("No hay archivos fallidos")
            
            with tab3:
                if files['processed']:
                    st.markdown("**Archivos ya procesados exitosamente:**")
                    for i, file in enumerate(files['processed'], 1):
                        st.markdown(f"<span class='file-status-processed'>{i}. {file}</span>", unsafe_allow_html=True)
                else:
                    st.info("No hay archivos procesados previamente")

        # Botón de procesamiento
        total_to_process = len(files['new']) + len(files['failed'])
        
        if total_to_process > 0:
            st.markdown("<h2 class='section-header'>🚀 Procesamiento</h2>", unsafe_allow_html=True)
            
            if st.button(f"🎯 Procesar {total_to_process} Archivos", type="primary", use_container_width=True, key="process_camp3"):
                
                # Contenedor para el progreso
                progress_container = st.container()
                log_container = st.container()
                
                with progress_container:
                    # Barra de progreso general
                    progress_bar = st.progress(0)
                    status_text = st.empty()
                    
                    # Métricas en tiempo real
                    col1, col2, col3 = st.columns(3)
                    with col1:
                        success_metric = st.empty()
                    with col2:
                        error_metric = st.empty()
                    with col3:
                        current_file = st.empty()

                with log_container:
                    st.markdown("### 📝 Log de Procesamiento")
                    log_placeholder = st.empty()
                    log_messages = []

                # Procesar archivos
                successful = 0
                failed = 0
                total_processed = 0
                
                # Procesar archivos nuevos
                for i, filename in enumerate(files['new']):
                    total_processed += 1
                    progress = total_processed / total_to_process
                    progress_bar.progress(progress)
                    status_text.text(f"Procesando archivo nuevo {i+1}/{len(files['new'])}: {filename}")
                    current_file.metric("📁 Archivo Actual", filename)
                    
                    log_messages.append(f"[{datetime.now().strftime('%H:%M:%S')}] Procesando: {filename}")
                    log_placeholder.markdown(
                        f"<div class='log-container'>{'<br>'.join(log_messages[-10:])}</div>", 
                        unsafe_allow_html=True
                    )
                    
                    # Dentro del bucle de procesamiento de archivos:
                    result = processor._process_single_file(
                        os.path.join(st.session_state.selected_folder_camp3, filename)
                    )

                    if result['success']:
                        if result['empty_module']:
                            log_messages.append(f"[{datetime.now().strftime('%H:%M:%S')}] ⓘ Archivo sin registros válidos (module='andes-movilidadtigo')")
                        else:
                            successful += 1
                            log_messages.append(f"[{datetime.now().strftime('%H:%M:%S')}] ✅ Éxito: {result['records']} registros")
                    else:
                        failed += 1
                        log_messages.append(f"[{datetime.now().strftime('%H:%M:%S')}] ❌ Error: {result['error'][:100]}...")
                
                # Procesar archivos fallidos (reintentos)
                for i, filename in enumerate(files['failed']):
                    total_processed += 1
                    progress = total_processed / total_to_process
                    progress_bar.progress(progress)
                    status_text.text(f"Reintentando archivo {i+1}/{len(files['failed'])}: {filename}")
                    current_file.metric("🔄 Reintentando", filename)
                    
                    log_messages.append(f"[{datetime.now().strftime('%H:%M:%S')}] Reintentando: {filename}")
                    log_placeholder.markdown(
                        f"<div class='log-container'>{'<br>'.join(log_messages[-10:])}</div>", 
                        unsafe_allow_html=True
                    )
                    
                    result = processor._process_single_file(
                        os.path.join(st.session_state.selected_folder_camp3, filename),
                        is_retry=True
                    )
                    
                    if result['success']:
                        successful += 1
                        log_messages.append(f"[{datetime.now().strftime('%H:%M:%S')}] ✅ Recuperado: {result['records']} registros")
                    else:
                        failed += 1
                        log_messages.append(f"[{datetime.now().strftime('%H:%M:%S')}] ❌ Falló nuevamente: {result['error'][:100]}...")
                    
                    success_metric.metric("✅ Exitosos", successful)
                    error_metric.metric("❌ Fallidos", failed)

                # Finalizar procesamiento
                progress_bar.progress(1.0)
                status_text.text("🎉 Procesamiento completado")
                current_file.empty()
                
                # Mostrar resumen final
                if successful == total_to_process:
                    st.markdown(f"""
                    <div class='success-box'>
                        <h3>🎉 Procesamiento Completado Exitosamente</h3>
                        <p><strong>Todos los archivos fueron procesados correctamente:</strong></p>
                        <ul>
                            <li>✅ Archivos exitosos: {successful}</li>
                            <li>📊 Total de archivos: {total_to_process}</li>
                        </ul>
                    </div>
                    """, unsafe_allow_html=True)
                elif successful > 0:
                    st.markdown(f"""
                    <div class='warning-box'>
                        <h3>⚠️ Procesamiento Completado con Advertencias</h3>
                        <p><strong>Resumen del procesamiento:</strong></p>
                        <ul>
                            <li>✅ Archivos exitosos: {successful}</li>
                            <li>❌ Archivos fallidos: {failed}</li>
                            <li>📊 Total procesados: {total_to_process}</li>
                        </ul>
                        <p><strong>Recomendación:</strong> Revisa los logs para identificar los errores en los archivos fallidos.</p>
                    </div>
                    """, unsafe_allow_html=True)
                else:
                    st.markdown(f"""
                    <div class='error-box'>
                        <h3>❌ Procesamiento Fallido</h3>
                        <p><strong>Ningún archivo pudo ser procesado exitosamente:</strong></p>
                        <ul>
                            <li>❌ Archivos fallidos: {failed}</li>
                            <li>📊 Total intentados: {total_to_process}</li>
                        </ul>
                        <p><strong>Sugerencias:</strong></p>
                        <ul>
                            <li>Verifica el formato de los archivos CSV</li>
                            <li>Revisa los logs de error para más detalles</li>
                            <li>Confirma que las columnas requeridas estén presentes</li>
                        </ul>
                    </div>
                    """, unsafe_allow_html=True)
                
                # Botón para limpiar y reiniciar
                if st.button("🔄 Procesar Nuevos Archivos", type="secondary", key="retry_camp3"):
                    st.rerun()
        
        elif total_to_process == 0 and files['processed']:
            st.markdown("""
            <div class='info-box'>
                <h3>ℹ️ Todos los Archivos Ya Están Procesados</h3>
                <p>Todos los archivos CSV en la carpeta seleccionada ya han sido procesados exitosamente.</p>
                <p><strong>Opciones:</strong></p>
                <ul>
                    <li>Selecciona una carpeta diferente con archivos nuevos</li>
                    <li>Agrega nuevos archivos CSV a la carpeta actual y actualiza</li>
                </ul>
            </div>
            """, unsafe_allow_html=True)
        
        else:
            st.markdown("""
            <div class='info-box'>
                <h3>📂 Carpeta Vacía</h3>
                <p>No se encontraron archivos CSV en la carpeta seleccionada.</p>
                <p><strong>Verifica que:</strong></p>
                <ul>
                    <li>La carpeta contenga archivos con extensión .csv</li>
                    <li>Los archivos no estén en subcarpetas</li>
                    <li>Tengas permisos de lectura en la carpeta</li>
                </ul>
            </div>
            """, unsafe_allow_html=True)

def main():
    """Función principal de la aplicación Streamlit"""
    
    # Sidebar con opciones
    with st.sidebar:
        st.markdown("**📌 Seleccione Tipo de Datos**")
        
        # Opciones en el sidebar
        option = st.radio(
            "Tipo de datos a procesar:",
            ["Tipificación 3", "CDR", "Campaña"],
            index=0
        )

    # Mostrar la UI correspondiente según la opción seleccionada
    if option == "Tipificación 3":
        show_tipificacion_3_ui()
    elif option == "CDR":
        show_cdr_ui()
    elif option == "Campaña":
        show_campana_ui()

    # Footer con información adicional
    st.markdown("---")
    st.markdown("""
    <div style='text-align: center; color: #666; padding: 20px 0;'>
        <p><strong>🗂️ Sistema SIAMM - Carga Wolkvox</strong></p>
        <p>Procesador de archivos CSV hacia PostgreSQL</p>
        <p><em>Versión Streamlit - Desarrollado para AndesBPO</em></p>
    </div>
    """, unsafe_allow_html=True)
    
if __name__ == "__main__":
    main()
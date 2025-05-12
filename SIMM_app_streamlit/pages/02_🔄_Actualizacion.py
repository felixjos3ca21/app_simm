import streamlit as st  

st.set_page_config(
    page_title="Carga y Actualización",
    page_icon="src/utils/favicon-114x114.png",
    layout="wide",
    initial_sidebar_state="expanded"
)

import pandas as pd
from datetime import datetime
from sqlalchemy import inspect, text
from src.database.postgres import get_engine
from src.utils.limpieza_archivo import preparar_datos
from src.utils.limpieza_sms import preparar_datos_sms
import os
from src.utils.limpieza_pagos import procesar_pagos
import tempfile
from src.utils.fondo import set_background

# ==============================================================================
# ESTILOS PERSONALIZADOS 
# ==============================================================================
st.markdown("""
    <style>
    /* Sidebar */
    [data-testid=stSidebar] {
        background: #f0f2f6;
        padding: 1rem;
        border-right: 1px solid #e1e4e8;
    }
    
    /* Radio buttons */
    div[role=radiogroup] {
        gap: 0.5rem;
    }
    
    /* Títulos */
    .sidebar .sidebar-title {
        color: #2c3e50;
        font-size: 1.2rem;
        margin-bottom: 1rem;
        font-weight: 600;
    }
    
    /* Instrucciones */
    .sidebar-instructions {
        color: #4a5568;
        font-size: 0.9rem;
        line-height: 1.5;
    }
    
    /* Hover effects */
    div[role=radiogroup] label:hover {
        background-color: #e2e8f0 !important;
    }
    </style>
""", unsafe_allow_html=True)
st.image("src/utils/logo-andesbpo-359x143.png", width=150)
set_background("src/utils/bg-seccion.png")
# ==============================================================================
# CLASE BASE: DataProcessor
# ==============================================================================
class DataProcessor:
    """Clase base para el procesamiento de datos"""
    
    def __init__(self, engine, config):
        self.engine = engine
        self.config = config
        self.df_procesado = None
        self.df_errores = None
        self.nuevos = None
        
    def _validar_estructura(self, df):
        """Valida que las columnas coincidan con la estructura de la BD"""
        inspector = inspect(self.engine)
        columnas_bd = [col['name'] for col in inspector.get_columns(self.config['table_name'])]
        columnas_archivo = df.columns.tolist()
        
        set_bd = set(columnas_bd)
        set_archivo = set(columnas_archivo)
        
        if set_bd != set_archivo:
            st.error("❌ Error de estructura: Las columnas no coinciden con la base de datos")
            st.stop()
            
        return True
    
    def _buscar_duplicados(self):
        """Integra la función utilitaria de verificación de duplicados"""
        nuevos = verificar_duplicados(
            self.engine,
            self.df_procesado,
            self.config['table_name'],
            self.config['id_column']
        )
        self.nuevos = nuevos
        self.duplicados = self.df_procesado.shape[0] - len(nuevos)
        
        st.write(f"📊 Registros nuevos detectados: {len(nuevos)}")
        st.write(f"📋 Registros duplicados detectados: {self.duplicados}")
        
        return nuevos
    
    def procesar_archivo(self, archivo):
        """Ejecuta solo el procesamiento y validación (sin carga)"""
        with st.status("🔄 Procesando archivo ...", expanded=True) as status:
            try:
                # Procesamiento individual
                self.df_procesado, self.df_errores, mensaje = self._procesar_archivo(archivo)
                
                if self.df_procesado.empty:
                    st.warning("⚠️ El archivo no contiene datos válidos")
                    return False
                
                # Validación estructural
                if not self._validar_estructura(self.df_procesado):
                    st.error("❌ Error en la estructura del archivo")
                    return False
                
                # Búsqueda de duplicados
                self.nuevos = self._buscar_duplicados()
                
                status.update(label="✅ Procesamiento completado", state="complete")
                return True
                
            except Exception as e:
                status.update(label="❌ Error en el proceso", state="error")
                st.error(f"Error crítico: {str(e)}")
                return False

    def _mostrar_resultados(self):
        """Muestra los resultados del procesamiento"""
        cols = st.columns(3)
        cols[0].metric("✅ Válidos", len(self.df_procesado))
        cols[1].metric("⚠️ Errores", len(self.df_errores))
        
    def _mostrar_errores(self):
        """Muestra y permite descargar los errores"""
        if not self.df_errores.empty:
            with st.expander("🚨 Detalle de errores", expanded=False):
                st.dataframe(self.df_errores)
                csv = self.df_errores.to_csv(index=False).encode('utf-8')
                st.download_button(
                    label="📥 Descargar reporte",
                    data=csv,
                    file_name=f"errores_{self.config['table_name']}.csv",
                    mime="text/csv"
                )
    
    def ejecutar_flujo(self, archivo):
        """Ejecuta el flujo completo de procesamiento"""
        with st.status(f"🏁 Procesando {self.config['table_name']}", expanded=True) as status:
            try:
                # Procesamiento inicial
                self.df_procesado, self.df_errores, mensaje = self._procesar_archivo(archivo)
                
                if self.df_procesado.empty:
                    st.warning(mensaje)
                    return
                
                # Validación estructural
                if not self._validar_estructura(self.df_procesado):
                    st.error("Error de estructura en los datos")
                    return
                
                # Búsqueda de duplicados
                self.nuevos = self._buscar_duplicados()
                
                # Carga de datos
                if not self.nuevos.empty:
                    self._cargar_datos()
                
                # Mostrar resultados finales
                self._mostrar_resultados()
                self._mostrar_errores()
                status.update(label="✅ Proceso completado", state="complete")
                
            except Exception as e:
                status.update(label="❌ Error en el proceso", state="error")
                st.error(f"Error crítico: {str(e)}")
                st.stop()
    
    def _cargar_datos(self):
        """Carga los datos en la base de datos con seguimiento detallado"""
        try:
            total_registros = len(self.nuevos)
            chunk_size = 5000
            chunks = [self.nuevos[i:i + chunk_size] 
                    for i in range(0, total_registros, chunk_size)]
            
            status_text = st.empty()
            progress_bar = st.progress(0)
            registros_insertados = 0
            
            with self.engine.begin() as conn:
                for i, chunk in enumerate(chunks):
                    # Actualizar progreso
                    progress = (i + 1) / len(chunks)
                    status_text.markdown(f"""
                        **Progreso de carga:**  
                        • Lotes procesados: `{i+1}/{len(chunks)}`  
                        • Registros insertados: `{registros_insertados + len(chunk)}/{total_registros}`
                    """)
                    
                    # Insertar chunk
                    chunk.to_sql(
                        name=self.config['table_name'],
                        con=conn,
                        if_exists='append',
                        index=False,
                        method='multi',
                        chunksize=500
                    )
                    
                    # Actualizar contadores
                    registros_insertados += len(chunk)
                    progress_bar.progress(progress)

            # Limpiar elementos de progreso
            progress_bar.empty()
            status_text.empty()
            
            st.cache_data.clear()

            # Mostrar resumen final
            st.success(f"**Carga exitosa:** {registros_insertados} registros nuevos insertados")
            st.metric("Tiempo promedio", f"{len(chunks)/60:.2f} registros/segundo")
            return True
            
        except Exception as e:
            progress_bar.empty()
            status_text.error(f"❌ Error en el lote {i+1}: {str(e)}")
            st.error("Se ha realizado rollback de la transacción")
            raise

# ==============================================================================
# CLASES ESPECÍFICAS POR MÓDULO
# ==============================================================================
class GestionesProcessor(DataProcessor):
    """Procesador específico para gestiones"""
        
    def __init__(self, engine):
        config = {
            'table_name': 'gestiones',
            'mapeo_columnas': {
                'id registro': 'id_registro',
                'Id Gestion Campaña': 'id_gestion_campaña',          
                'Tipo documento': 'tipo_documento',
                'Número documento': 'documento',
                'Nombre': 'nombre_usuario',
                'Fecha gestión': 'fecha_gestion',
                'Tipo llamada': 'tipo_llamada',
                'Código gestión': 'id_gestion',
                'Resultado': 'resultado',
                'Fecha Compromiso': 'fecha_compromiso',
                'Funcionario': 'asesor',
                'Campaña': 'campana',
                'Teléfono': 'telefono',
                'Obligación': 'obligacion',
                'Nro. Comparendo': 'numero_comparendo',
                'Valor': 'valor',
                'identificador_infraccion': 'identificador_infraccion',
                'archivo_origen': 'archivo_origen',
                'fecha_carga': 'fecha_carga',
                'fecha_gestion_sencilla': 'fecha_gestion_sencilla'
            },
            'id_column': 'id_registro',
            'clean_function': preparar_datos
        }
        super().__init__(engine, config)
        
    def _procesar_archivo(self, archivo):
        try:
            df_procesado, df_errores, mensaje = preparar_datos(archivo, archivo.name)
            
            # Validación adicional
            campos_obligatorios = ['tipo_documento', 'nombre_usuario', 'documento', 'fecha_gestion']
            mask_errores = df_procesado[campos_obligatorios].isnull().any(axis=1)
            
            # Mover registros con errores
            nuevos_errores = df_procesado[mask_errores].copy()
            df_procesado = df_procesado[~mask_errores]
            
            # Combinar errores nuevos con los originales
            if not nuevos_errores.empty:
                nuevos_errores['error'] = 'Campos obligatorios faltantes'
                df_errores = pd.concat([df_errores, nuevos_errores])
            
            return df_procesado, df_errores, mensaje
        except Exception as e:
            return pd.DataFrame(), pd.DataFrame({'error': [str(e)]}), str(e)

class SMSProcessor(DataProcessor):
    """Procesador específico para SMS"""
    
    def __init__(self, engine):
        config = {
            'table_name': 'sms',
            'mapeo_columnas': {
                'id_registro': 'id_registro',
                'TIPO DOCUMENTO': 'tipo_documento',
                'DOCUMENTO': 'documento',
                'NOMBRE': 'nombre_usuario',
                'FECHA': 'fecha_sms',
                'RESULTADO': 'resultado',
                'SMS': 'texto_sms',
                'BASE': 'base',
                'TELEFONO': 'telefono',
                'NRO_COMPARENDO': 'numero_comparendo',
                'identificador_infraccion': 'identificador_infraccion',
                'archivo_origen': 'archivo_origen',
                'fecha_carga': 'fecha_carga'
            },
            'id_column': 'id_registro',
            'clean_function': preparar_datos_sms
        }
        super().__init__(engine, config)
        
    def _procesar_archivo(self, archivo):
        """Implementación específica de limpieza para SMS"""
        return preparar_datos_sms(archivo, archivo.name)
    
class PagosProcessor(DataProcessor):
    """Procesador específico para pagos con mejor manejo de múltiples archivos"""
    
    def __init__(self, engine):
        config = {
            'table_name': 'pagos',
            'mapeo_columnas': {
                'id_registro': 'id_registro',
                'nro_acuerdo': 'nro_acuerdo',
                'nro_comparendo': 'nro_comparendo',
                'documento': 'documento',
                'nombre_usuario': 'nombre_usuario',
                'valor': 'valor',
                'fecha_pago': 'fecha_pago',
                'archivo_origen': 'archivo_origen',
                'identificador_infraccion': 'identificador_infraccion',
                'fecha_carga': 'fecha_carga'
            },
            'id_column': 'id_registro',
            'clean_function': procesar_pagos
        }
        super().__init__(engine, config)
        self.archivos_procesados = []
        
    def _procesar_archivo(self, archivo):
        """Implementación mejorada para manejar múltiples archivos"""
        try:
            # Crear directorio temporal si no existe
            temp_dir = os.path.join(tempfile.gettempdir(), "simm_pagos")
            os.makedirs(temp_dir, exist_ok=True)
            
            # Manejar tanto UploadedFile como rutas directas
            if hasattr(archivo, 'name'):  # Es un UploadedFile de Streamlit
                temp_path = os.path.join(temp_dir, archivo.name)
                with open(temp_path, 'wb') as f:
                    f.write(archivo.getvalue())
                nombre_archivo = archivo.name
            else:
                temp_path = archivo
                nombre_archivo = os.path.basename(archivo)
            
            # Procesar el archivo
            df_procesado, df_errores, mensaje = procesar_pagos(temp_path, nombre_archivo)
            
            # Limpiar archivo temporal si lo creamos
            if hasattr(archivo, 'name'):
                try:
                    os.unlink(temp_path)
                except:
                    pass
            
            if df_procesado.empty:
                return pd.DataFrame(), df_errores, mensaje
            
            # Asegurar columnas requeridas
            columnas_requeridas = [
                'id_registro', 'nro_acuerdo', 'nro_comparendo', 'documento',
                'nombre_usuario', 'valor', 'fecha_pago', 'archivo_origen',
                'identificador_infraccion', 'fecha_carga'
            ]
            
            for col in columnas_requeridas:
                if col not in df_procesado.columns:
                    df_procesado[col] = None
            
            # Registrar archivo procesado
            self.archivos_procesados.append(nombre_archivo)
            
            return df_procesado, df_errores, mensaje
            
        except Exception as e:
            error_msg = f"Error procesando {getattr(archivo, 'name', archivo)}: {str(e)}"
            return pd.DataFrame(), pd.DataFrame({'error': [error_msg]}), error_msg

# ==============================================================================
# FUNCIONES UTILITARIAS
# ==============================================================================
@st.cache_resource
def get_db_connection():
    """Obtiene y cachea la conexión a la base de datos"""
    try:
        engine = get_engine()
        # Prueba simple de conexión
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        return engine
    except Exception as e:
        st.error(f"❌ Error de conexión a la base de datos: {str(e)}")
        st.stop()

def verificar_duplicados(engine, df, table_name, id_column):
    """Verifica registros duplicados en la base de datos"""
    try:
        chunk_size = 1000
        ids = df[id_column].unique().tolist()
        chunks = [ids[i:i + chunk_size] for i in range(0, len(ids), chunk_size)]
        
        existing_ids = set()
        progress_bar = st.progress(0)
        
        with engine.connect() as conn:
            for i, chunk in enumerate(chunks):
                progress = (i + 1) / len(chunks)
                progress_bar.progress(progress)
                
                result = conn.execute(
                    text(f"SELECT {id_column} FROM {table_name} WHERE {id_column} = ANY(:ids)"),
                    {'ids': chunk}
                )
                existing_ids.update([row[0] for row in result])
        
        return df[~df[id_column].isin(existing_ids)]
    
    except Exception as e:
        st.error(f"Error en verificación de duplicados: {str(e)}")
        st.stop()

# ==============================================================================
# INTERFAZ DE USUARIO StreamlitUI
# ==============================================================================
class StreamlitUI:
    """Manejador principal de la interfaz de usuario"""
    
    def __init__(self):
        self.engine = get_db_connection()
        self._reset_session_state()
        
    def _reset_session_state(self):
        """Resetea el estado de la sesión cuando cambia el módulo"""
        if 'last_module' not in st.session_state:
            st.session_state.last_module = None
            
        current_module = st.session_state.get('modulo_actual', None)
        
        # Si el módulo ha cambiado, resetear el estado
        if st.session_state.last_module != current_module:
            st.session_state.update({
                'procesado': False,
                'processor': None,
                'current_file_index': 0,
                'all_files': None,
                'total_files': 0,
                'last_module': current_module
            })
    
    def _mostrar_sidebar(self):
        """Muestra la barra lateral de navegación con estilos mejorados"""
        MODULOS = {
            "Carga de Gestiones": "🧮",
            "Carga de SMS": "📲",
            "Carga de Pagos": "💰"
        }
        
        with st.sidebar:
            st.markdown('<div class="sidebar-title">Módulos Disponibles</div>', unsafe_allow_html=True)
            
            # Usamos una clave única para el radio button
            modulo_seleccionado = st.radio(
                "Seleccione el módulo:",
                options=list(MODULOS.keys()),
                index=0,
                format_func=lambda x: f"{MODULOS[x]} {x}",
                key="modulo_actual"  # Clave importante para el seguimiento
            )
            
            # Actualizar el módulo actual
            self.modulo = modulo_seleccionado
            
            st.markdown("---")
            st.markdown('<div class="sidebar-title">Instrucciones</div>', unsafe_allow_html=True)
            st.markdown("""
                <div class="sidebar-instructions">
                1. Seleccione el módulo correspondiente<br>
                2. Cargue el archivo <br>
                3. Siga el proceso de validación
                </div>
            """, unsafe_allow_html=True)
    
    def _mostrar_carga_archivo(self):
        """Componente de carga de archivo con reset automático"""
        # Verificar si necesitamos resetear
        self._reset_session_state()
        
        with st.container():
            if self.modulo:
                MODULO_ICONOS = {
                    "Carga de Gestiones": "🧮",
                    "Carga de SMS": "📲",
                    "Carga de Pagos": "💰"
                }
                icono = MODULO_ICONOS.get(self.modulo, "📁")
                st.title(f"{icono} {self.modulo}")
                
                # Definir tipo de archivo según módulo
                tipo = "xlsx" if self.modulo != "Carga de Pagos" else ["txt"]
                
                # Permitir múltiples archivos solo para pagos
                multiple = self.modulo == "Carga de Pagos"
                
                # Usamos una clave única por módulo para el file_uploader
                uploaded_files = st.file_uploader(
                    f"Subir archivo{'s' if multiple else ''} {'Excel' if tipo == 'xlsx' else 'TXT'}",
                    type=tipo,
                    key=f"upload_{self.modulo}",  # Clave única por módulo
                    help=f"Archivo {'Excel' if tipo == 'xlsx' else 'texto'} con extensión .{tipo}",
                    accept_multiple_files=multiple
                )
                
                return uploaded_files
        return None

    def ejecutar(self):
        """Ejecuta la aplicación principal con manejo robusto de errores"""
        self._mostrar_sidebar()
        uploaded_files = self._mostrar_carga_archivo()
        
        if uploaded_files:
            # Convertir a lista si es un solo archivo
            files_to_process = uploaded_files if isinstance(uploaded_files, list) else [uploaded_files]
            
            # Reiniciar estado al cargar nuevos archivos
            if 'procesado' not in st.session_state:
                st.session_state.update({
                    'procesado': False,
                    'processor': None,
                    'current_file_index': 0
                })
            
            col1, col2 = st.columns([2, 3])
            
            with col1:
                st.subheader("⚙ Procesar Archivo")
                if st.button("✅ Confirmar Procesar Archivo", type="primary"):
                    # Mapeo de módulos a clases processor
                    MODULO_PROCESSORS = {
                        "Carga de Gestiones": GestionesProcessor,
                        "Carga de SMS": SMSProcessor,
                        "Carga de Pagos": PagosProcessor
                    }
                    
                    processor_class = MODULO_PROCESSORS.get(self.modulo)
                    
                    if processor_class is None:
                        st.error("❌ Módulo no reconocido")
                        return
                    
                    try:
                        # Procesar el primer archivo
                        processor = processor_class(self.engine)
                        success = processor.procesar_archivo(files_to_process[0])
                        
                        if success:
                            st.session_state.update({
                                'procesado': True,
                                'processor': processor,
                                'all_files': files_to_process,
                                'current_file_index': 0,
                                'total_files': len(files_to_process),
                                'processor_class': processor_class  # Guardar la clase para reutilizar
                            })
                            st.rerun()
                        else:
                            st.error("Error en el procesamiento inicial")
                    except Exception as e:
                        st.error(f"Error al procesar archivo: {str(e)}")
                        st.stop()
            
            if st.session_state.procesado and st.session_state.processor is not None:
                processor = st.session_state.processor
                current_file = st.session_state.all_files[st.session_state.current_file_index]
                
                # Mostrar progreso de múltiples archivos
                if st.session_state.total_files > 1:
                    st.progress((st.session_state.current_file_index + 1) / st.session_state.total_files)
                    st.caption(f"Archivo {st.session_state.current_file_index + 1} de {st.session_state.total_files}: {current_file.name}")
                
                # Mostrar resultados del procesamiento
                with col2:
                    st.subheader("📊 Resultados del Procesamiento")
                    if not processor.df_procesado.empty:
                        cols = st.columns(4)
                        cols[0].metric("✅ Válidos", len(processor.df_procesado))
                        cols[1].metric("🆕 Nuevos", len(processor.nuevos))
                        cols[2].metric("📋 Duplicados", processor.duplicados)
                        cols[3].metric("❌ Errores", len(processor.df_errores))
                    else:
                        st.warning("No hay datos procesados válidos")
                
                # Sección de navegación para múltiples archivos
                if st.session_state.total_files > 1:
                    cols = st.columns([1, 2, 1])
                    with cols[0]:
                        if st.session_state.current_file_index > 0 and st.button("⏮ Anterior"):
                            st.session_state.current_file_index -= 1
                            st.rerun()
                    
                    with cols[1]:
                        st.caption(f"Progreso: {st.session_state.current_file_index + 1}/{st.session_state.total_files}")
                    
                    with cols[2]:
                        if st.session_state.current_file_index < st.session_state.total_files - 1 and st.button("Siguiente ⏭"):
                            try:
                                # Usar la clase guardada en session_state
                                next_processor = st.session_state.processor_class(self.engine)
                                next_file = st.session_state.all_files[st.session_state.current_file_index + 1]
                                success = next_processor.procesar_archivo(next_file)
                                
                                if success:
                                    st.session_state.update({
                                        'processor': next_processor,
                                        'current_file_index': st.session_state.current_file_index + 1
                                    })
                                    st.rerun()
                                else:
                                    st.error("Error al procesar el siguiente archivo")
                            except Exception as e:
                                st.error(f"Error al cambiar de archivo: {str(e)}")
                
                # Sección de carga condicional
                if len(processor.nuevos) > 0:
                    st.divider()
                    st.subheader("🚀 Carga de Datos")
                    
                    if st.button("✅ Confirmar e Iniciar Carga", 
                            type="primary", 
                            help="Haz clic para cargar los datos en la base de datos"):
                        try:
                            if processor._cargar_datos():
                                st.success("✅ Datos cargados exitosamente")
                                
                                # Si hay más archivos, preparar el siguiente
                                if st.session_state.current_file_index < st.session_state.total_files - 1:
                                    st.session_state.procesado = False
                                    st.rerun()
                                else:
                                    st.balloons()
                                    st.session_state.procesado = False
                        except Exception as e:
                            st.error(f"Error durante la carga: {str(e)}")
                
                # Mostrar errores si existen
                if not processor.df_errores.empty:
                    with st.expander("🚨 Detalle de Errores", expanded=True):
                        # Agregar columna de motivo del error si no existe
                        if 'error' not in processor.df_errores.columns:
                            processor.df_errores['error'] = 'Desconocido'
                            
                        # Mostrar resumen primero
                        st.write("**Resumen de errores:**")
                        error_counts = processor.df_errores['error'].value_counts()
                        st.dataframe(error_counts.reset_index().rename(columns={'index': 'Tipo de error', 'error': 'Cantidad'}))
                        
                        # Mostrar detalles
                        st.write("**Registros con errores:**")
                        st.dataframe(processor.df_errores)
                        
                        # Botón de descarga
                        csv = processor.df_errores.to_csv(index=False).encode('utf-8')
                        st.download_button(
                            label="📥 Descargar errores completos",
                            data=csv,
                            file_name="errores_detallados.csv",
                            mime="text/csv"
                        )
# ==============================================================================
# EJECUCIÓN PRINCIPAL
# ==============================================================================
if __name__ == "__main__":
    app = StreamlitUI()
    app.ejecutar()
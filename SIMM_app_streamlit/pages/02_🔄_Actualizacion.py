import streamlit as st
import pandas as pd
import os
import tempfile
from datetime import datetime
from sqlalchemy import inspect, text
from src.database.postgres import get_engine, DatabaseManager
from src.utils.limpieza_archivo import preparar_datos
from src.utils.limpieza_sms import preparar_datos_sms
from src.utils.limpieza_pagos import procesar_pagos
from src.utils.limpieza_bases import preparar_datos_bases
from src.utils.fondo import set_background
import time


# ==============================================================================
# CONFIGURACIÓN INICIAL
# ==============================================================================
st.set_page_config(
    page_title="SIAMM - Carga y Actualización",
    page_icon="src/utils/favicon-114x114.png",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Estilos CSS
st.markdown("""
    <style>
    [data-testid=stSidebar] {
        background-color: #A6DEB7 !important;
    }
    .main-container {
        padding: 2rem;
    }
    div[role=radiogroup] {
        gap: 0.5rem;
    }
    .sidebar .sidebar-title {
        color: #2c3e50;
        font-size: 1.2rem;
        margin-bottom: 1rem;
        font-weight: 600;
    }
    .sidebar-instructions {
        color: #4a5568;
        font-size: 0.9rem;
        line-height: 1.5;
    }
    div[role=radiogroup] label:hover {
        background-color: #e2e8f0 !important;
    }
    </style>
""", unsafe_allow_html=True)

st.image("src/utils/logo-andesbpo-359x143.png", width=150)
set_background("src/utils/bg-seccion.png")

engine = DatabaseManager.get_engine('SIMM')

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
                try:
                    conn.execute(text(f"ALTER TABLE {self.config['table_name']} DISABLE TRIGGER ALL"))
                except:
                    pass
                
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
                        chunksize=chunk_size
                    )
                    
                    registros_insertados += len(chunk)
                    progress_bar.progress(progress)
                
                # Rehabilitar índices
                try:
                    conn.execute(text(f"ALTER TABLE {self.config['table_name']} ENABLE TRIGGER ALL"))
                except:
                    pass
                    
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
    """Procesador específico para pagos con procesamiento por lotes"""
    
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
        self.resultados_procesamiento = []
        self.df_resumen = pd.DataFrame()
        
    def _procesar_archivo(self, archivo):
        """Implementación específica de limpieza para pagos"""
        try:
            # Validación adicional del tipo de archivo
            if hasattr(archivo, 'name') and not archivo.name.lower().endswith('.txt'):
                return pd.DataFrame(), pd.DataFrame({'error': ['El archivo debe ser .txt']}), "Formato inválido"
            # Crear directorio temporal si no existe
            temp_dir = os.path.join(tempfile.gettempdir(), "simm_pagos")
            os.makedirs(temp_dir, exist_ok=True)
            
            # Manejar tanto UploadedFile como rutas directas
            if hasattr(archivo, 'name'):  
                temp_path = os.path.join(temp_dir, archivo.name)
                with open(temp_path, 'wb') as f:
                    f.write(archivo.getvalue())
                nombre_archivo = archivo.name
            else:
                temp_path = archivo
                nombre_archivo = os.path.basename(archivo)
            
            # Procesar el archivo usando la función utilitaria
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
        
    def procesar_archivos_multiples(self, archivos):
        """Procesa múltiples archivos y combina sus resultados"""
        total_archivos = len(archivos)
        
        with st.status(f"🔄 Procesando {total_archivos} archivos...", expanded=True) as status:
            self.df_procesado = pd.DataFrame()
            self.df_errores = pd.DataFrame()
            
            # Crear dataframe para almacenar resumen
            self.df_resumen = pd.DataFrame(columns=[
                'nombre_archivo', 'registros_totales', 'registros_validos',
                'registros_nuevos', 'registros_duplicados', 'registros_con_errores'
            ])
            
            # Procesar cada archivo y acumular resultados
            for idx, archivo in enumerate(archivos):
                status.update(label=f"🔄 Procesando archivo {idx+1}/{total_archivos}: {archivo.name}")
                
                # Procesar archivo individual
                df_actual, df_errores_actual, mensaje = self._procesar_archivo(archivo)
                
                if not df_actual.empty:
                    # Guardar resultados de este archivo
                    df_actual['archivo_origen'] = archivo.name
                    
                    # Verificar duplicados para este archivo
                    nuevos_actual = verificar_duplicados(
                        self.engine,
                        df_actual,
                        self.config['table_name'],
                        self.config['id_column']
                    )
                    duplicados_actual = df_actual.shape[0] - len(nuevos_actual)
                    
                    # Acumular resultados
                    self.df_procesado = pd.concat([self.df_procesado, df_actual])
                    
                    # Guardar resumen
                    self.df_resumen.loc[len(self.df_resumen)] = {
                        'nombre_archivo': archivo.name,
                        'registros_totales': len(df_actual) + len(df_errores_actual),
                        'registros_validos': len(df_actual),
                        'registros_nuevos': len(nuevos_actual),
                        'registros_duplicados': duplicados_actual,
                        'registros_con_errores': len(df_errores_actual)
                    }
                
                if not df_errores_actual.empty:
                    df_errores_actual['archivo'] = archivo.name
                    self.df_errores = pd.concat([self.df_errores, df_errores_actual])
            
            # Buscar duplicados en el conjunto final consolidado
            if not self.df_procesado.empty:
                self.nuevos = verificar_duplicados(
                    self.engine,
                    self.df_procesado,
                    self.config['table_name'],
                    self.config['id_column']
                )
                self.duplicados = self.df_procesado.shape[0] - len(self.nuevos)
                
                status.update(label=f"✅ Procesamiento completado: {len(self.nuevos)} registros nuevos", state="complete")
                return True
            else:
                status.update(label="⚠️ No se encontraron registros válidos", state="error")
                return False
                
    def mostrar_resumen_procesamiento(self):
        """Muestra una tabla resumen del procesamiento de todos los archivos"""
        if not self.df_resumen.empty:
            st.subheader("📊 Resumen de procesamiento por archivo")
            
            # Formateamos para mejor visualización
            df_display = self.df_resumen.copy()
            df_display = df_display.style.format({
                'registros_totales': '{:,.0f}',
                'registros_validos': '{:,.0f}',
                'registros_nuevos': '{:,.0f}',
                'registros_duplicados': '{:,.0f}',
                'registros_con_errores': '{:,.0f}'
            })
            
            st.dataframe(df_display, use_container_width=True)
            
            # Totales generales
            col1, col2, col3, col4, col5 = st.columns(5)
            col1.metric("📄 Total Archivos", len(self.df_resumen))
            col2.metric("✅ Total Registros Válidos", self.df_resumen['registros_validos'].sum())
            col3.metric("🆕 Total Registros Nuevos", self.df_resumen['registros_nuevos'].sum())
            col4.metric("📋 Total Registros Duplicados", self.df_resumen['registros_duplicados'].sum())
            col5.metric("⚠️ Total Errores", self.df_resumen['registros_con_errores'].sum())
    
    def _cargar_todos_datos(self):
        """Carga todos los datos nuevos en la base de datos de una sola vez"""
        if not hasattr(self, 'nuevos') or self.nuevos.empty:
            st.warning("No hay datos nuevos para cargar")
            return False
            
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
            return True
            
        except Exception as e:
            st.error(f"❌ Error en la carga: {str(e)}")
            raise

class BasesProcessor(DataProcessor):
    """Procesador específico para Bases"""

    def __init__(self, engine):
        config = {
            'table_name': 'bases',
            'mapeo_columnas': {
                'id_registro': 'id_registro',
                'BASE': 'base',
                'FECHA DE ENTREGA': 'fecha_entrega',
                'TIPO DE DCTO': 'tipo_documento',
                'IDENTIFICACIÓN': 'documento',
                'NOMBRE': 'nombre',
                'NRO. COMPARENDO': 'numero_comparendo',
                'FECHA DE COMPARENDO': 'fecha_comparendo',
                'CODIGO DE INFRACCIÓN': 'codigo_infraccion',
                'PLACA': 'placa',
                'valor': 'valor_infraccion',
                'CEL1': 'telefono',
                'archivo_origen': 'archivo_origen',
                'fecha_carga': 'fecha_carga'
            },
            'id_column': 'id_registro',
            'clean_function': preparar_datos_bases
        }
        super().__init__(engine, config)

    def _procesar_archivo(self, archivo):
        """
        Llama a la función de limpieza y aplica el mapeo de columnas
        para que coincida con la estructura de la tabla 'bases'.
        """
        df_procesado, df_errores, mensaje = preparar_datos_bases(archivo, archivo.name)

        # Aplicar mapeo de columnas a df_procesado
        mapeo = self.config["mapeo_columnas"]
        df_procesado = df_procesado.rename(columns=mapeo)

        # Aplicar mapeo a df_errores si contiene columnas relevantes
        if not df_errores.empty:
            df_errores = df_errores.rename(columns={k: v for k, v in mapeo.items() if k in df_errores.columns})

        return df_procesado, df_errores, mensaje

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
# INTERFAZ DE USUARIO - CORRECCIONES PRINCIPALES
# ==============================================================================
class StreamlitUI:
    def __init__(self):
        self.engine = get_db_connection()
        self._initialize_session_state()
            
    def _initialize_session_state(self):
        """Inicializa el estado de la sesión"""
        if 'modulo_actual' not in st.session_state:
            st.session_state.modulo_actual = "Carga de Gestiones"
        if 'uploaded_files' not in st.session_state:
            st.session_state.uploaded_files = None
        if 'procesado' not in st.session_state:
            st.session_state.procesado = False
        if 'processor' not in st.session_state:
            st.session_state.processor = None
            
    def _reset_upload_state(self):
        """Resetea el estado relacionado con la carga de archivos"""
        st.session_state.uploaded_files = None
        st.session_state.procesado = False
        st.session_state.processor = None
        
    def _mostrar_sidebar(self):
        """Muestra la barra lateral de navegación"""
        MODULOS = {
            "Carga de Gestiones": "🧮",
            "Carga de SMS": "📲",
            "Carga de Pagos": "💰",
            "Carga de Bases": "📋"
        }
        
        with st.sidebar:
            st.header("Módulos Disponibles")
            
            # Usamos on_change para resetear el estado cuando cambia el módulo
            modulo_seleccionado = st.radio(
                "Seleccione el módulo:",
                options=list(MODULOS.keys()),
                index=list(MODULOS.keys()).index(st.session_state.modulo_actual),
                format_func=lambda x: f"{MODULOS[x]} {x}",
                key="modulo_radio",
                on_change=self._reset_upload_state
            )
            
            st.session_state.modulo_actual = modulo_seleccionado
            
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
        """Componente de carga de archivo mejorado"""
        MODULO_CONFIG = {
            "Carga de Gestiones": {"extensions": ["xlsx"], "multiple": False, "icon": "🧮"},
            "Carga de SMS": {"extensions": ["xlsx"], "multiple": False, "icon": "📲"}, 
            "Carga de Pagos": {"extensions": ["txt"], "multiple": True, "icon": "💰"},
            "Carga de Bases": {"extensions": ["xlsx"], "multiple": False, "icon": "📋"}
        }
        
        config = MODULO_CONFIG.get(st.session_state.modulo_actual, {})
        
        st.title(f"{config.get('icon', '📄')} {st.session_state.modulo_actual}")
        
        # Crear una key única que cambie cuando se resetea el estado
        upload_key = f"uploader_{st.session_state.modulo_actual}_{hash(str(st.session_state.get('reset_timestamp', 0)))}"
        
        # Solución especial para el módulo de Pagos
        if st.session_state.modulo_actual == "Carga de Pagos":
            uploaded_files = st.file_uploader(
                "Subir archivos TXT",
                type=["txt"],  # Cambiado: usar string en lugar de lista
                accept_multiple_files=True,
                key=upload_key,
                help="Solo se aceptan archivos con extensión .txt"
            )
        else:
            uploaded_files = st.file_uploader(
                f"Subir archivo{'s' if config['multiple'] else ''}",
                type=config["extensions"],
                accept_multiple_files=config["multiple"],
                key=upload_key
            )
        
        if uploaded_files:
            # Convertir a lista si no lo es
            if not isinstance(uploaded_files, list):
                uploaded_files = [uploaded_files]
                
            # Validación adicional para archivos TXT
            if st.session_state.modulo_actual == "Carga de Pagos":
                valid_files = []
                for file in uploaded_files:
                    if file.name.lower().endswith('.txt'):
                        valid_files.append(file)
                    else:
                        st.warning(f"Archivo {file.name} ignorado. Solo se aceptan .txt")
                st.session_state.uploaded_files = valid_files if valid_files else None
            else:
                st.session_state.uploaded_files = uploaded_files
            
            return len(st.session_state.uploaded_files) > 0 if st.session_state.uploaded_files else False
        return False
    
    def ejecutar(self):
        """Ejecuta la aplicación principal con correcciones"""
        self._mostrar_sidebar()
        
        if st.button("🔄 Limpiar todo"):
            self._reset_upload_state()
            st.rerun()
            
        if self._mostrar_carga_archivo():
            files = st.session_state.uploaded_files
            if not isinstance(files, list):
                files = [files]
                
            st.success(f"✅ Archivo{'s' if len(files)>1 else ''} cargado{'s' if len(files)>1 else ''} correctamente")
            
            col1, col2 = st.columns([2, 3])
            
            with col1:
                st.subheader("⚙ Procesar Archivo")
                if st.button("✅ Confirmar Procesar Archivo", type="primary"):
                    MODULO_PROCESSORS = {
                        "Carga de Gestiones": GestionesProcessor,
                        "Carga de SMS": SMSProcessor,
                        "Carga de Pagos": PagosProcessor,
                        "Carga de Bases": BasesProcessor
                    }
                    
                    processor_class = MODULO_PROCESSORS.get(st.session_state.modulo_actual)
                    processor = processor_class(self.engine)
                    
                    try:
                        if st.session_state.modulo_actual == "Carga de Pagos" and len(files) > 1:
                            success = processor.procesar_archivos_multiples(files)
                        else:
                            success = processor.procesar_archivo(files[0])
                            
                        if success:
                            st.session_state.procesado = True
                            st.session_state.processor = processor
                            st.rerun()
                            
                    except Exception as e:
                        st.error(f"Error al procesar: {str(e)}")
            
            if st.session_state.procesado and st.session_state.processor:
                processor = st.session_state.processor
                self._mostrar_resultados(processor)
    
    def _mostrar_resultados(self, processor):
        """Muestra los resultados del procesamiento"""
        col1, col2 = st.columns([2, 3])
        
        with col2:
            st.subheader("📊 Resultados del Procesamiento")
            
            if hasattr(processor, 'df_procesado') and not processor.df_procesado.empty:
                cols = st.columns(4)
                cols[0].metric("✅ Válidos", len(processor.df_procesado))
                cols[1].metric("🆕 Nuevos", len(getattr(processor, 'nuevos', pd.DataFrame())))
                cols[2].metric("📋 Duplicados", getattr(processor, 'duplicados', 0))
                cols[3].metric("❌ Errores", len(getattr(processor, 'df_errores', pd.DataFrame())))
            
            if hasattr(processor, 'nuevos') and len(processor.nuevos) > 0:
                st.divider()
                st.subheader("🚀 Carga de Datos")
                st.info(f"Se cargarán {len(processor.nuevos)} registros nuevos")
                
                if st.button("✅ Confirmar e Iniciar Carga", type="primary"):
                    try:
                        if st.session_state.modulo_actual == "Carga de Pagos":
                            processor._cargar_todos_datos()
                        else:
                            processor._cargar_datos()
                        st.success("✅ Datos cargados exitosamente")
                        st.balloons()
                        self._reset_upload_state()
                    except Exception as e:
                        st.error(f"Error en carga: {str(e)}")

# ==============================================================================
# EJECUCIÓN PRINCIPAL
# ==============================================================================
if __name__ == "__main__":
    app = StreamlitUI()
    app.ejecutar()
import streamlit as st  

# Configuración de página DEBE SER EL PRIMER COMANDO STREAMLIT
st.set_page_config(
    page_title="Carga y Actualización",
    page_icon="🔄",
    layout="wide",
    initial_sidebar_state="expanded"
)


import pandas as pd
import time
from datetime import datetime
from sqlalchemy import inspect, text
from src.database.postgres import get_engine
from src.utils.limpieza_archivo import preparar_datos
from src.utils.limpieza_sms import preparar_datos_sms

# ==============================================================================
# ESTILOS PERSONALIZADOS (agregar después de set_page_config)
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
                # Paso 1: Procesamiento inicial
                self.df_procesado, self.df_errores, mensaje = self._procesar_archivo(archivo)
                
                if self.df_procesado.empty:
                    st.warning("⚠️ El archivo no contiene datos válidos")
                    return False
                
                # Paso 2: Validación estructural
                if not self._validar_estructura(self.df_procesado):
                    st.error("❌ Error en la estructura del archivo")
                    return False
                
                # Paso 3: Búsqueda de duplicados
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
                # Paso 1: Procesamiento inicial
                self.df_procesado, self.df_errores, mensaje  = self._procesar_archivo(archivo)
                
                if self.df_procesado.empty:
                    st.warning(mensaje)
                    return
                
                # Paso 2: Validación estructural
                if not self._validar_estructura(self.df_procesado):
                    st.error("Error de estructura en los datos")
                    return
                
                # Paso 3: Búsqueda de duplicados
                self.nuevos = self._buscar_duplicados()
                
                # Paso 4: Carga de datos
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
                    
                    # Log detallado (opcional)
                    #st.write(f"✅ Lote {i+1} insertado: {len(chunk)} registros")

            # Limpiar elementos de progreso
            progress_bar.empty()
            status_text.empty()
            
            # Mostrar resumen final
            st.success(f"**Carga exitosa:** {registros_insertados} registros nuevos insertados")
            st.metric("Tiempo promedio", f"{len(chunks)/60:.2f} registros/segundo")
            return True
            
        except Exception as e:
            progress_bar.empty()
            status_text.error(f"❌ Error en el lote {i+1}: {str(e)}")
            st.error("Se ha realizado rollback de la transacción")
            raise  # Propagar el error para manejo superior

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
                'fecha_carga': 'fecha_carga'
            },
            'id_column': 'id_registro',
            'clean_function': preparar_datos
        }
        super().__init__(engine, config)
        
    def _procesar_archivo(self, archivo):
        """Implementación específica de limpieza para gestiones"""
        df_procesado, df_errores, mensaje = preparar_datos(archivo, archivo.name)
        df_renombrado = df_procesado.rename(columns=self.config['mapeo_columnas'])
        return df_renombrado, df_errores, mensaje  # Asegurar 3 valores de retorno

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

# ==============================================================================
# FUNCIONES UTILITARIAS
# ==============================================================================
@st.cache_resource
def get_db_connection():
    """Obtiene y cachea la conexión a la base de datos"""
    try:
        engine = get_engine()
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        return engine
    except Exception as e:
        st.error(f"Error de conexión: {str(e)}")
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
        self.modulo = None  
        
    def _mostrar_sidebar(self):
        """Muestra la barra lateral de navegación con estilos mejorados"""
        with st.sidebar:
            # Título con estilo personalizado
            st.markdown('<div class="sidebar-title">Módulos Disponibles</div>', unsafe_allow_html=True)
            
            self.modulo = st.radio(
                "Seleccione el módulo:",
                options=["Carga de Gestiones", "Carga de SMS"],
                index=0
            )
            
            st.markdown("---")
            
            # Sección de instrucciones
            st.markdown('<div class="sidebar-title">Instrucciones</div>', unsafe_allow_html=True)
            st.markdown("""
                <div class="sidebar-instructions">
                1. Seleccione el módulo correspondiente<br>
                2. Cargue el archivo Excel<br>
                3. Siga el proceso de validación
                </div>
            """, unsafe_allow_html=True)
        
    
    def _mostrar_carga_archivo(self):
        """Muestra el componente de carga de archivo"""
        with st.container():
            if self.modulo:  # Verificar que no sea None
                icono = '🧮' if 'gestiones' in self.modulo.lower() else '📲'
                st.title(f"{icono} {self.modulo}")
                return st.file_uploader(
                    f"Cargar archivo para {self.modulo}",
                    type=["xlsx"],
                    key=f"upload_{self.modulo}"
                )
            return None
    
    def ejecutar(self):
        """Ejecuta la aplicación principal con el nuevo flujo"""
        self._mostrar_sidebar()
        archivo = self._mostrar_carga_archivo()
        
        if archivo:
            # Reiniciar estado al cargar nuevo archivo
            if 'procesado' not in st.session_state:
                st.session_state.procesado = False
                st.session_state.processor = None
            
            # Columna para controles de proceso
            col1, col2 = st.columns([2, 3])
            
            with col1:
                st.subheader("⚙ Procesar Archivo")
                if st.button("✅ Confirmar Procesar Archivo", type="primary"):
                    if "gestiones" in self.modulo.lower():
                        processor = GestionesProcessor(self.engine)
                    else:
                        processor = SMSProcessor(self.engine)
                    
                    if processor.procesar_archivo(archivo):
                        st.session_state.procesado = True
                        st.session_state.processor = processor
                        st.rerun()
            
            if st.session_state.procesado:
                processor = st.session_state.processor
                
                # Mostrar resultados del procesamiento
                with col2:
                    st.subheader("📊 Resultados del Procesamiento")
                    st.metric("✅ Registros válidos  ", len(processor.df_procesado))
                    st.metric("🆕 Registros nuevos", len(processor.nuevos))
                    st.metric("📋 Registros duplicados", processor.duplicados)
                    st.metric("❌ Errores detectados", len(processor.df_errores))
                
                # Sección de carga condicional
                if len(processor.nuevos) > 0:
                    st.divider()
                    st.subheader("🚀 Carga de Datos")
                    
                    if st.button("✅ Confirmar e Iniciar Carga", 
                               type="primary", 
                               help="Haz clic para cargar los datos en la base de datos"):
                        if processor._cargar_datos():
                            st.balloons()
                            st.session_state.procesado = False  # Resetear estado
                
                # Mostrar errores si existen
                if not processor.df_errores.empty:
                    with st.expander("🚨 Detalle de Errores", expanded=False):
                        st.dataframe(processor.df_errores)
                        csv = processor.df_errores.to_csv(index=False).encode('utf-8')
                        st.download_button(
                            label="📥 Descargar errores",
                            data=csv,
                            file_name="errores_procesamiento.csv",
                            mime="text/csv"
                        )

# ==============================================================================
# EJECUCIÓN PRINCIPAL
# ==============================================================================
if __name__ == "__main__":
    app = StreamlitUI()
    app.ejecutar()
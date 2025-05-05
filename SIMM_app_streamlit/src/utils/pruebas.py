# import streamlit as st
# import pandas as pd
# import time
# from datetime import datetime
# from src.database.postgres import get_engine
# from src.utils.limpieza_archivo import preparar_datos
# from sqlalchemy import inspect, text, MetaData
# import numpy as np

# def mostrar_progreso(progreso: float, mensaje: str):
#     progress_bar = st.progress(progreso)
#     progress_bar.write(mensaje)

# # 1. Configuración de la página
# st.set_page_config(
#     page_title="Actualización Gestiones",
#     page_icon="🔄",
#     layout="wide"
# )

# # 3. Conexión a PostgreSQL con verificación
# @st.cache_resource
# def get_db_connection():
#     try:
#         engine = get_engine()
#         with engine.connect() as conn:
#             conn.execute(text("SELECT 1"))
#         return engine
#     except Exception as e:
#         st.error(f"❌ Error de conexión: {str(e)}")
#         st.stop()

# engine = get_db_connection()

# # 4. Mapeo de columnas
# MAPEO_COLUMNAS = {
#     'id registro': 'id_registro',
#     'Id Gestion Campaña': 'id_gestion_campaña',          
#     'Tipo documento': 'tipo_documento',
#     'Número documento': 'documento',
#     'Nombre': 'nombre_usuario',
#     'Fecha gestión': 'fecha_gestion',
#     'Tipo llamada': 'tipo_llamada',
#     'Código gestión': 'id_gestion',
#     'Resultado': 'resultado',
#     'Fecha Compromiso': 'fecha_compromiso',
#     'Funcionario': 'asesor',
#     'Campaña': 'campana',
#     'Teléfono': 'telefono',
#     'Obligación': 'obligacion',
#     'Nro. Comparendo': 'numero_comparendo',
#     'Valor': 'valor',
#     'identificador_infraccion': 'identificador_infraccion',
#     'archivo_origen': 'archivo_origen',
#     'fecha_carga': 'fecha_carga'
# }

# # 5. Función de validación de estructura
# def validar_estructura_bd(df):
#     inspector = inspect(engine)
#     columnas_bd = [col['name'] for col in inspector.get_columns('gestiones')]
    
#     # Verificar columnas faltantes
#     columnas_faltantes = set(df.columns) - set(columnas_bd)
#     if columnas_faltantes:
#         st.error(f"🚨 Columnas que no existen en la base de datos: {columnas_faltantes}")
#         st.stop()
    
#     return True

# # 6. Interfaz principal
# st.title("🔄 Cargador de Gestiones")
# st.markdown("Carga archivos Excel con múltiples hojas para actualizar la base de datos")

# # 7. Widget de carga de archivo
# with st.expander("⚙️ Configuración de carga", expanded=True):
#     archivo = st.file_uploader(
#         "Seleccione archivo Excel",
#         type=["xlsx"],
#         help="El archivo debe contener al menos una hoja con las columnas requeridas"
#     )


# if archivo:
#     try:
#         start_time = time.time()
#         status_container = st.empty()
        
#         with st.status("🏁 **Progreso General**", expanded=True) as status:
#             # Mover la definición de actualizar_progreso_limpieza aquí
#             def actualizar_progreso_limpieza(progreso, mensaje):
#                 with status_container:
#                     mostrar_progreso(progreso, mensaje)
            
#             try:
#                 # Etapa 1: Procesamiento del archivo
#                 st.write("📤 **Etapa 1: Validación de Hojas**")
                
#                 with st.spinner('🔍 Analizando archivo...'):
#                     df_procesado, df_errores, mensaje = preparar_datos(
#                         archivo, 
#                         archivo.name,
#                         update_progress=actualizar_progreso_limpieza
#                     )
                    
#                     if df_procesado.empty:
#                         st.warning("⚠️ El archivo no contiene registros válidos")
#                         st.stop()

#                     # Aplicar mapeo de columnas
#                     df_final = df_procesado.rename(columns=MAPEO_COLUMNAS)
#                     validar_estructura_bd(df_final)

#                 # Etapa 2: Resultados de validación
#                 st.write("✅ **Etapa 2: Validación Exitosa**")
#                 st.success(f"Registros aptos: {len(df_procesado)}")
                
#                 # Etapa 3: Búsqueda de Duplicados (Versión optimizada)
#                 st.write("🔄 **Etapa 3: Búsqueda de Duplicados**")
#                 with st.spinner("Consultando base de datos..."):
#                     try:
#                         ids_a_verificar = df_final['id_registro'].unique()
                        
#                         if len(ids_a_verificar) == 0:
#                             st.warning("No hay IDs para verificar")
#                             st.stop()
                        
#                         chunk_size = 1000
#                         chunks = [ids_a_verificar[i:i + chunk_size] 
#                                 for i in range(0, len(ids_a_verificar), chunk_size)]
                        
#                         # Configurar progreso único
#                         progress_bar = st.progress(0)
#                         status_text = st.empty()
#                         existing_ids = set()
#                         total_chunks = len(chunks)
#                         update_interval = max(1, total_chunks // 20)  # Actualizar cada 5%
                        
#                         with engine.connect() as conn:
#                             for i, chunk in enumerate(chunks):
#                                 # Actualizar progreso solo en intervalos o último chunk
#                                 if i % update_interval == 0 or i == total_chunks - 1:
#                                     progress = (i + 1) / total_chunks
#                                     progress_bar.progress(progress)
#                                     status_text.markdown(f"""
#                                         **Progreso de verificación:**  
#                                         • Lotes procesados: `{i+1}/{total_chunks}`  
#                                         • IDs verificados: `{(i+1)*chunk_size}`  
#                                         • Porcentaje completado: `{progress*100:.1f}%`
#                                     """)
                                
#                                 # Consulta optimizada
#                                 result = conn.execute(
#                                     text("SELECT id_registro FROM gestiones WHERE id_registro = ANY(:ids)"),
#                                     {'ids': chunk.tolist()}
#                                 )
#                                 existing_ids.update([row[0] for row in result])
                        
#                         # Ocultar barra y mensaje temporal
#                         progress_bar.empty()
#                         status_text.empty()
                        
#                         # Mostrar resultado final
#                         nuevos = df_final[~df_final['id_registro'].isin(existing_ids)]
#                         st.success(f"""
#                             **Verificación completada:**  
#                             ✅ Registros nuevos: `{len(nuevos)}`  
#                             🔄 Registros existentes: `{len(df_final) - len(nuevos)}`
#                         """)

#                     except Exception as e:
#                         st.error(f"❌ Error en verificación de duplicados: {str(e)}")
#                         st.stop()
#                 # Etapa 4: Carga de datos
#                 st.write("🚀 **Etapa 4: Carga de Datos**")
#                 if len(nuevos) > 0:
#                     if st.button("Iniciar carga completa", type="primary"):
#                         try:
#                             total_registros = len(nuevos)
#                             chunk_size = 500
#                             chunks = [nuevos[i:i + chunk_size] 
#                                     for i in range(0, total_registros, chunk_size)]
                            
#                             progress_bar = st.progress(0)
#                             status_text = st.empty()
#                             start_carga = time.time()
#                             registros_insertados = 0
                            
#                             with engine.begin() as conn:
#                                 for i, chunk in enumerate(chunks):
#                                     # Calcular métricas
#                                     progress = (i + 1) / len(chunks)
#                                     registros_insertados += len(chunk)
#                                     elapsed_time = time.time() - start_carga
#                                     velocidad_promedio = registros_insertados / elapsed_time if elapsed_time > 0 else 0
                                    
#                                     # Actualizar cada 1% o último chunk
#                                     if i % max(1, len(chunks)//100) == 0 or i == len(chunks)-1:
#                                         progress_bar.progress(progress)
#                                         status_text.markdown(f"""
#                                             **Progreso:**  
#                                             • Lotes procesados: `{i+1}/{len(chunks)}`  
#                                             • Registros insertados: `{registros_insertados}`  
#                                             • Velocidad promedio: `{velocidad_promedio:.2f} regs/seg`  
#                                             • Tiempo transcurrido: `{elapsed_time:.2f}s`
#                                         """)
                                    
#                                     # Insertar chunk
#                                     chunk.to_sql(
#                                         name='gestiones',
#                                         con=conn,
#                                         if_exists='append',
#                                         index=False,
#                                         method='multi',
#                                         chunksize=100
#                                     )
                            
#                             # Resultado final
#                             st.balloons()
#                             st.success(f"✅ Carga exitosa: {registros_insertados} registros insertados")
#                             st.metric("Tiempo total", f"{(time.time() - start_carga):.2f} segundos")
#                             status.update(label="✅ **Proceso Completado**", state="complete")
                            
#                         except Exception as e:
#                             st.error(f"❌ Error en carga: {str(e)}")
#                             st.session_state.nuevos_registros -= registros_insertados  # Revertir conteo
#                             st.stop()
            
#             except ValueError as e:
#                 if "Ninguna hoja válida encontrada" in str(e):
#                     st.error("❌ **Error en estructura del archivo:**")
#                     st.write(str(e))
#                     st.stop()
#                 else:
#                     raise

#         # Sección de errores
#         if not df_errores.empty:
#             with st.expander("🚨 Detalle de errores", expanded=False):
#                 st.write(mensaje)
#                 st.dataframe(df_errores, use_container_width=True)
                
#                 csv = df_errores.to_csv(index=False).encode('utf-8')
#                 st.download_button(
#                     label="📥 Descargar reporte de errores",
#                     data=csv,
#                     file_name=f"errores_{datetime.now().strftime('%Y%m%d')}.csv",
#                     mime="text/csv"
#                 )

#     except Exception as e:
#         st.error(f"🚨 Error crítico: {str(e)}")
#         st.stop()


# ##-------------------------------------------------------------------------------------------------------------------
# ##-------------------------------------------------------------------------------------------------------------------



# import streamlit as st
# import pandas as pd
# import time
# from datetime import datetime
# from src.database.postgres import get_engine
# from src.utils.limpieza_sms import preparar_datos_sms
# from sqlalchemy import inspect, text
# from sqlalchemy.exc import SQLAlchemyError

# # 1. Configuración inicial
# st.set_page_config(
#     page_title="Gestor de SMS - Cargador",
#     page_icon="📲",
#     layout="wide",
#     initial_sidebar_state="expanded"
# )

# # Estilos personalizados
# st.markdown("""
# <style>
#     [data-testid=stMetricLabel] { font-size: 0.9rem !important; }
#     .stProgress > div > div > div { background-color: #25C3B0; }
#     .st-emotion-cache-16idsys p { color: #2ecc71; }
# </style>
# """, unsafe_allow_html=True)

# # 2. Header
# with st.container():
#     col_logo, col_title = st.columns([0.1, 0.9])
#     with col_logo:
#         st.image("https://cdn-icons-png.flaticon.com/512/126/126341.png", width=50)
#     with col_title:
#         st.title("📲 Cargador Masivo de SMS")
#         st.caption("Versión 2.0 | Carga segura de mensajes SMS a la base de datos operacional")

# # 3. Conexión a BD optimizada
# @st.cache_resource
# def get_db_connection():
#     try:
#         engine = get_engine()
#         engine.connect()  # Test de conexión inicial
#         return engine
#     except Exception as e:
#         st.error(f"❌ Error de conexión: {str(e)}")
#         st.stop()

# engine = get_db_connection()

# # 4. Mapeo de columnas
# MAPEO_COLUMNAS_SMS = {
#     'id_registro': 'id_registro',
#     'TIPO DOCUMENTO': 'tipo_documento',
#     'DOCUMENTO': 'documento',
#     'NOMBRE': 'nombre_usuario',
#     'FECHA': 'fecha_sms',
#     'RESULTADO': 'resultado',
#     'SMS': 'texto_sms',
#     'BASE': 'base',
#     'TELEFONO': 'telefono',
#     'NRO_COMPARENDO': 'numero_comparendo',
#     'identificador_infraccion': 'identificador_infraccion',
#     'archivo_origen': 'archivo_origen',
#     'fecha_carga': 'fecha_carga'
# }

# # 5. Componente de errores
# def mostrar_errores(df, mensaje):
#     with st.expander("🚨 **Errores Detectados - Acción Requerida**", expanded=True):
#         st.error(mensaje)
#         st.dataframe(
#             df.head(10),
#             use_container_width=True,
#             column_config={
#                 "telefono": "Teléfono",
#                 "documento": st.column_config.NumberColumn("Documento", format="%d")
#             }
#         )
#         st.download_button(
#             label="⬇️ Exportar Reporte Completo",
#             data=df.to_csv(index=False).encode('utf-8'),
#             file_name=f"errores_sms_{datetime.now().strftime('%Y%m%d_%H%M')}.csv",
#             mime="text/csv",
#             type="primary"
#         )

# # 6. Flujo principal mejorado
# archivo = st.file_uploader(
#     "**Arrastra tu archivo Excel o haz clic para buscar**",
#     type=["xlsx"],
#     help="""**Formato requerido:**\n
#     - Columnas obligatorias: TIPO DOCUMENTO, DOCUMENTO, TELEFONO\n
#     - Máximo 200MB\n
#     - Formato fecha: YYYY-MM-DD"""
# )

# if archivo:
#     with st.status("📦 **Proceso de Carga - En tiempo real**", expanded=True) as status:
#         try:
#             # ----------------------------------
#             # 0. Verificación inicial de conexión
#             # ----------------------------------
#             with st.spinner("🔌 Verificando conexión a la base de datos..."):
#                 with engine.connect() as conn:
#                     conn.execute(text("SELECT 1"))
#                 st.success("✅ Conexión activa y estable")
#                 time.sleep(0.5)

#             # ----------------------------------
#             # 1. Lectura de datos
#             # ----------------------------------
#             with st.spinner("🔍 Analizando estructura del archivo..."):
#                 df_raw = pd.read_excel(archivo)
#                 st.success(f"📥 Datos crudos cargados ({len(df_raw):,} registros)")
                
#                 with st.container():
#                     st.caption("📋 Muestra de datos (3 primeros registros)")
#                     st.dataframe(df_raw.head(3), use_container_width=True)
#                 time.sleep(0.3)

#             # ----------------------------------
#             # 2. Procesamiento de datos
#             # ----------------------------------
#             with st.spinner("🧼 Limpiando y validando datos..."):
#                 df_procesado, df_errores, _ = preparar_datos_sms(archivo, archivo.name)
                
#                 cols = st.columns(3)
#                 cols[0].metric("✅ Válidos", f"{len(df_procesado):,}")
#                 cols[1].metric("⚠️ Errores", f"{len(df_errores):,}")
#                 cols[2].metric("📅 Última fecha", df_procesado['fecha_sms'].max().strftime("%Y-%m-%d"))
                
#                 if not df_errores.empty:
#                     mostrar_errores(df_errores, "Detalles de errores")
#                 time.sleep(0.3)

#             # ----------------------------------
#             # 3. Validación estructural
#             # ----------------------------------
#             with st.spinner("🔍 Comparando con estructura de la BD..."):
#                 df_final = df_procesado.rename(columns=MAPEO_COLUMNAS_SMS)
#                 inspector = inspect(engine)
#                 columnas_bd = [col['name'] for col in inspector.get_columns('sms')]
                
#                 if set(df_final.columns) != set(columnas_bd):
#                     st.error("❌ Error de estructura: Columnas no coinciden")
#                     st.stop()
#                 st.success("✅ Estructura compatible")
#                 time.sleep(0.3)

#             # ----------------------------------
#             # 4. Gestión de duplicados
#             # ----------------------------------
#             with st.spinner("🦠 Buscando duplicados..."):
#                 ids = df_final['id_registro'].unique().tolist()
#                 with engine.connect() as conn:
#                     existing_ids = pd.read_sql(
#                         text("SELECT id_registro FROM sms WHERE id_registro = ANY(:ids)"), 
#                         conn, 
#                         params={'ids': ids}
#                     )['id_registro'].tolist()
                
#                 nuevos = df_final[~df_final['id_registro'].isin(existing_ids)]
                
#                 cols_dup = st.columns(3)
#                 cols_dup[0].metric("🆔 Únicos", f"{len(ids):,}")
#                 cols_dup[1].metric("♻️ Duplicados", f"{len(existing_ids):,}")
#                 cols_dup[2].metric("🆕 Nuevos", f"{len(nuevos):,}")
#                 time.sleep(0.3)

#             # ----------------------------------
#             # 5. Carga condicional (Cambio clave)
#             # ----------------------------------
#             if len(nuevos) > 0:
#                 st.subheader("🚀 Acción Final Requerida")
#                 with st.container(border=True):
#                     cols_confirm = st.columns([0.7, 0.3])
#                     cols_confirm[0].markdown(f"""
#                         **Confirmar carga:**  
#                         ⚠️ Esta acción es irreversible  
#                         ✔️ Registros a insertar: **{len(nuevos):,}**  
#                         🕒 Tiempo estimado: **{max(1, round(len(nuevos)/1000, 1))} segundos**
#                     """)
                    
#                     if cols_confirm[1].button("⚠️ **EJECUTAR CARGA**", 
#                                              type="primary", 
#                                              use_container_width=True):
#                         with st.spinner("⏳ Procesando..."):
#                             start_time = time.time()
#                             with engine.begin() as conn:
#                                 nuevos.to_sql(
#                                     name='sms',
#                                     con=conn,
#                                     if_exists='append',
#                                     index=False
#                                 )
#                         st.balloons()
#                         st.success(f"✅ Carga exitosa: {len(nuevos):,} SMS nuevos")
#                         status.update(label="🏁 **Carga Completa**", state="complete")
#             else:
#                 st.warning("📭 No hay nuevos registros para cargar")
#                 status.update(label="🏁 **Proceso Finalizado**", state="complete")

#         except Exception as e:
#             status.update(label="❌ **Error Crítico**", state="error")
#             st.error(f"Fallo en el proceso: {str(e)}")
#             st.stop()

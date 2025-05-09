import pandas as pd
from datetime import datetime
import hashlib
import numpy as np
from typing import Tuple

def preparar_datos(ruta_arcivo, nombre_archivo, update_progress=None) -> Tuple[pd.DataFrame, pd.DataFrame, str]:
    """
    Versión mejorada con todas las validaciones requeridas
    """
    try:
        # =============================================================================
        # 1. Configuración inicial y lectura
        # =============================================================================
        total_steps = 13
        current_step = 0
        
        def update_step(message: str):
                nonlocal current_step
                current_step += 1
                progress = current_step / total_steps
                # Asegurar que no exceda 100%
                progress = min(progress, 1.0)
                if update_progress:
                    update_progress(progress, f"Paso {current_step}/{total_steps}: {message}")

        update_step("Iniciando procesamiento")
        
        # Columnas obligatorias
        columnas_requeridas = {
            'Id Gestion Campaña', 'Tipo documento', 'Número documento', 'Nombre',
            'Fecha gestión', 'Tipo llamada', 'Código gestión', 'Resultado',
            'Fecha Compromiso', 'Funcionario', 'Campaña', 'Teléfono',
            'Obligación', 'Nro. Comparendo', 'Valor'
        }

        # =============================================================================
        # 2. Lectura y validación de hojas
        # =============================================================================
        update_step("Leyendo archivo Excel")
        hojas = pd.read_excel(ruta_arcivo, sheet_name=None)
        
        update_step("Validando estructura de hojas")
        hojas_validas = []
        errores_hojas = []
        
        for hoja, datos in hojas.items():
            faltantes = columnas_requeridas - set(datos.columns)
            if not faltantes:
                hojas_validas.append(datos)
            else:
                errores_hojas.append(f"Hoja '{hoja}': Faltan {', '.join(faltantes)}")
        
        if not hojas_validas:
            raise ValueError(f"Ninguna hoja válida:\n" + "\n".join(errores_hojas))

        # =============================================================================
        # 3. Unificación y limpieza inicial
        # =============================================================================
        update_step("Unificando hojas válidas")
        df = pd.concat(hojas_validas, ignore_index=True).replace({
            '': pd.NA, ' ': pd.NA, 'NULL': pd.NA, 'nan': pd.NA, 'NaN': pd.NA
        })

        # =============================================================================
        # 4. Generación de Código Gestión (si está vacío)
        # =============================================================================
        update_step("Generando códigos de gestión")
        mask = df['Código gestión'].isna() | (df['Código gestión'] == '')
        df.loc[mask, 'Código gestión'] = (
            df['Número documento'].astype(str).str.strip() + '_' +
            df['Teléfono'].astype(str).str.strip()
        )

        # =============================================================================
        # 5. Validación de fechas
        # =============================================================================
        update_step("Procesando fechas")
        
        # Fecha gestión debe ser datetime
        df['Fecha gestión'] = pd.to_datetime(
            df['Fecha gestión'],
            errors='coerce',
            format='%Y-%m-%d %H:%M:%S'
        )
        
        # Fecha compromiso solo fecha
        df['Fecha Compromiso'] = pd.to_datetime(
            df['Fecha Compromiso'],
            errors='coerce'
        ).dt.normalize()  # Elimina componente horario

        # =============================================================================
        # 6. Generación de identificador_infraccion
        # =============================================================================
        update_step("Generando identificador de infracción")
        df['identificador_infraccion'] = np.where(
            df['Obligación'].notna(),
            df['Obligación'].astype(str),
            df['Nro. Comparendo'].astype(str)
        )

        # =============================================================================
        # 7. Generación de ID único
        # =============================================================================
        update_step("Creando ID único")
        
        grupo = [
            'Número documento', 
            'Teléfono', 
            'Funcionario', 
            'Fecha gestión',
            'Código gestión', 
            'identificador_infraccion', 
            'Resultado'
        ]
        
        df['contador_grupo'] = df.groupby(grupo).cumcount() + 1
        
        df['id registro'] = df.apply(
            lambda x: hashlib.sha256((
                f"{x['Número documento']}_"
                f"{x['Teléfono']}_"
                f"{x['Funcionario']}_"
                f"{x['Fecha gestión'].timestamp() if pd.notnull(x['Fecha gestión']) else ''}_"
                f"{x['Código gestión']}_"
                f"{x['identificador_infraccion']}_"
                f"{x['Resultado']}_"
                f"{x['contador_grupo']}"
            ).encode()).hexdigest(),
            axis=1
        )
        df.drop('contador_grupo', axis=1, inplace=True)

        # =============================================================================
        # 8. Validación final de datos
        # =============================================================================
        update_step("Validando integridad")
        
        # Verificar IDs únicos
        if df['id registro'].duplicated().any():
            duplicados = df[df.duplicated('id registro', keep=False)]
            raise ValueError(f"IDs duplicados:\n{duplicados[['Número documento', 'Teléfono']]}")
            
        # Verificar fechas válidas
        if df['Fecha gestión'].isna().any():
            raise ValueError("Fecha gestión contiene valores no válidos")

        # =============================================================================
        # 9. Separación de registros válidos/erróneos
        # =============================================================================
        update_step("Clasificando registros")
        
        # Campos obligatorios para considerar válido
        campos_obligatorios = [
            'Número documento', 'Teléfono', 'Funcionario', 
            'Fecha gestión', 'Código gestión'
        ]
        
        mask_errores = df[campos_obligatorios].isna().any(axis=1)
        df_errores = df[mask_errores].copy()
        df_procesado = df[~mask_errores].copy()

        # =============================================================================
        # 10. Normalización de nombres de columnas
        # =============================================================================
        update_step("Normalizando nombres de columnas")
        
        mapeo_columnas = {
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
            'id registro': 'id_registro',
            
        }
        
        df_procesado = df_procesado.rename(columns=mapeo_columnas)
        df_errores = df_errores.rename(columns=mapeo_columnas)

        # =============================================================================
        # 11. Metadatos finales y fecha_gestion_sencilla
        # =============================================================================
        update_step("Agregando metadatos")
        
        # Generar fecha_gestion_sencilla usando la columna ya renombrada 'fecha_gestion'
        df_procesado['fecha_gestion_sencilla'] = df_procesado['fecha_gestion'].dt.strftime('%Y-%m-%d')
        df_procesado['archivo_origen'] = nombre_archivo
        df_procesado['fecha_carga'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        df_procesado['id_gestion'] = df_procesado['id_gestion'].astype(str) 
        # Convertir id_gestion_campaña a string
        df_procesado['id_gestion_campaña'] = df_procesado['id_gestion_campaña'].astype(str)
        df_errores['archivo_origen'] = nombre_archivo
        df_errores['fecha_carga'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        # =============================================================================
        # 12. Asegurar columnas necesarias
        # =============================================================================
        update_step("Asegurando columnas finales")
        
        columnas_finales = [
            'id_registro', 'id_gestion_campaña', 'tipo_documento', 'documento',
            'nombre_usuario', 'fecha_gestion', 'tipo_llamada', 'id_gestion',
            'resultado', 'fecha_compromiso', 'asesor', 'campana', 'telefono',
            'obligacion', 'numero_comparendo', 'valor', 'identificador_infraccion',
            'archivo_origen', 'fecha_carga', 'fecha_gestion_sencilla'
        ]
        
        # Asegurar que todas las columnas existan
        df_procesado = df_procesado.reindex(columns=columnas_finales)
        df_errores = df_errores.reindex(columns=columnas_finales + ['fecha_gestion_sencilla'])
        update_step("Verificación final de datos")  # Paso 13
        return df_procesado, df_errores, "\n".join(errores_hojas)
        
        

    except Exception as e:
        raise ValueError(f"Error en paso {current_step}: {str(e)}")
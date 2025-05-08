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
        total_steps = 12
        current_step = 0
        
        def update_step(message: str):
            nonlocal current_step
            current_step += 1
            if update_progress:
                progress = current_step / total_steps
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
        # 10. Metadatos finales
        # =============================================================================
        update_step("Agregando metadatos")
        df_procesado['archivo_origen'] = nombre_archivo
        df_procesado['fecha_carga'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        df_procesado['fecha_gestion_sencilla'] = df_procesado['Fecha gestión'].dt.strftime('%Y-%m-%d')
        df_errores['archivo_origen'] = nombre_archivo
        df_errores['fecha_carga'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        return df_procesado, df_errores, "\n".join(errores_hojas)

    except Exception as e:
        raise ValueError(f"Error en paso {current_step}: {str(e)}")
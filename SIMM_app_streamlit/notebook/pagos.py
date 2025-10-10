import os
import pandas as pd
import tkinter as tk
from tkinter import filedialog, messagebox
from tqdm import tqdm

# Función para seleccionar archivos
def seleccionar_archivo(titulo):
    root = tk.Tk()
    root.withdraw()
    archivo = filedialog.askopenfilename(title=titulo)
    if not archivo:
        messagebox.showerror("Error", f"No se seleccionó un archivo para {titulo}")
        exit()
    return archivo

# Seleccionar archivos
ruta_pagos = seleccionar_archivo("Selecciona el archivo de pagos")
ruta_gestiones = seleccionar_archivo("Selecciona el archivo de gestiones")
ruta_exporte = filedialog.asksaveasfilename(title="Guardar archivo como", defaultextension=".xlsx", filetypes=[("Excel Files", "*.xlsx")])

# Cargar datos con manejo de errores
try:
    print("Cargando datos...")
    df_pagos = pd.read_excel(ruta_pagos, sheet_name="BD PAGOS", usecols=["codcliente", "nitcliente", "numobligacion", "FECHA SENCILLA", "valorpago", "APLICACIÓN FINAL"])
    df_gestiones = pd.read_csv(ruta_gestiones, usecols=["NUMERO_DOCUMENTO", "FECHA", "HORA_REGISTRO", "TIPOLLAMADA", "ID_REGISTRO_GESTION", "NOMBRE_RESULTADO", "FECHA_COMPROMISO_PAGO", "FUNCIONARIO", "NUMERO_OBLIGACION", "NRO_COMPARENDO"])
except Exception as e:
    messagebox.showerror("Error", f"Hubo un problema al cargar los archivos: {e}")
    exit()

# Filtrar solo pagos con "APLICA" y para nit especifico Bancolombia
df_pagos = df_pagos[
    (df_pagos["APLICACIÓN FINAL"] == "APLICA") & 
    (df_pagos["nitcliente"] != '890903938')
].copy()

# Asegurarse de que la fecha esté en formato datetime
df_pagos["FECHA SENCILLA"] = pd.to_datetime(df_pagos["FECHA SENCILLA"], dayfirst=True)

# Filtrar pagos con "APLICA" 
df_pagos = df_pagos[
    (df_pagos["APLICACIÓN FINAL"] == "APLICA") &
    (df_pagos["FECHA SENCILLA"] >= "2025-10-09") ## MODIFICAR A DIARIO ######
].copy()

# Filtrar gestiones válidas
resultados_validos = {
    "Compromiso de pago": 1,
    "Compromiso de acuerdo de pago": 2,
    "Caso Especial": 3,
    "No Define Fecha De Pago": 4,
    "Sin voluntad de pago": 5,
    "Mensaje con terceros": 6,
    "Mensaje": 7,
    "Volver a llamar": 8,
}


# resultados_validos = {
#     "Compromiso de pago": 1,
#     "Compromiso de acuerdo de pago": 2,
#     "Caso Especial": 3,
#     "No Define Fecha De Pago": 4,
#     "Sin voluntad de pago": 5,
#     "Mensaje con terceros": 6,
#     "Mensaje": 7,
#     "Volver a llamar": 8,
#     "Nro. inhabilitado": 9,
#     "No contestan": 10,
#     "Ocupado": 11,
#     "Entrega Comunicado": 12,
#     "Nuevos Datos": 13,
#     "Equivocado": 14,
#     "Fallecido": 15,
#     "Conmutador": 16,
#     "No localizado": 18,
#     "Envio De E-Mail": 19,
#     "Audiencia": 20
# }
df_gestiones = df_gestiones[df_gestiones["NOMBRE_RESULTADO"].isin(resultados_validos.keys())].copy()

# Convertir fechas a formato datetime
df_pagos["FECHA SENCILLA"] = pd.to_datetime(df_pagos["FECHA SENCILLA"], dayfirst=True)
df_gestiones["FECHA"] = pd.to_datetime(df_gestiones["FECHA"], dayfirst=True)

# Función para encontrar gestiones relacionadas
def encontrar_gestiones(pago, df_gestiones):
    # Convertir a string para comparación más segura
    num_obligacion_pago = str(pago['numobligacion'])
    nit_cliente_pago = str(pago['nitcliente'])
    
    # Filtrar por documento primero
    gestiones_cliente = df_gestiones[
        df_gestiones['NUMERO_DOCUMENTO'].astype(str) == nit_cliente_pago
    ].copy()
    
    if gestiones_cliente.empty:
        return pd.DataFrame()
    
    # Buscar por obligación específica primero
    mask_obligacion_exacta = (
        (gestiones_cliente['NRO_COMPARENDO'].astype(str) == num_obligacion_pago) |
        (gestiones_cliente['NUMERO_OBLIGACION'].astype(str) == num_obligacion_pago)
    )
    
    gestiones_obligacion = gestiones_cliente[mask_obligacion_exacta]
    
    # Si no encuentra por obligación exacta, usar todas las gestiones del cliente
    if gestiones_obligacion.empty:
        gestiones_relacionadas = gestiones_cliente
    else:
        gestiones_relacionadas = gestiones_obligacion
    
    # Filtrar solo gestiones anteriores o del mismo día del pago
    gestiones_relacionadas = gestiones_relacionadas[
        gestiones_relacionadas['FECHA'] <= pago['FECHA SENCILLA']
    ].copy()
    
    return gestiones_relacionadas

# Función para convertir hora entera a formato HH:MM:SS
def convertir_hora_entera(hora_int):
    try:
        hora_int = int(float(hora_int))  # Convertir a entero (manejando posibles floats)
        return f"{hora_int:02d}:00:00"  # Formato de 2 dígitos con minutos y segundos en 00
    except:
        return "00:00:00"  # Valor por defecto si hay error

# Asignar pagos a gestores (versión corregida)
print("Asignando pagos...")
asignaciones = []
fechas_asignadas = []
resultados_asignados = []
horas_asignadas = []

for _, pago in tqdm(df_pagos.iterrows(), total=len(df_pagos), desc="Procesando pagos"):
    gestiones_relacionadas = encontrar_gestiones(pago, df_gestiones)
    
    if gestiones_relacionadas.empty:
        asignaciones.append("SIN ASESOR ASIGNADO")
        fechas_asignadas.append("")
        resultados_asignados.append("")
        horas_asignadas.append("")
    else:
        # Convertir horas enteras a formato HH:MM:SS
        gestiones_relacionadas["HORA_FORMATEADA"] = gestiones_relacionadas["HORA_REGISTRO"].apply(convertir_hora_entera)
        # Crear columna combinada de fecha y hora
        gestiones_relacionadas["FECHA_HORA"] = pd.to_datetime(
            gestiones_relacionadas["FECHA"].dt.strftime('%Y-%m-%d') + " " +
            gestiones_relacionadas["HORA_FORMATEADA"]
        )
        # Asignar importancia según el resultado
        gestiones_relacionadas["IMPORTANCIA"] = gestiones_relacionadas["NOMBRE_RESULTADO"].map(resultados_validos)
        # Ordenar por importancia (mejor primero)
        gestiones_ordenadas = gestiones_relacionadas.sort_values(
            by=["IMPORTANCIA"], ascending=[True]
        )
        # Tomar solo las gestiones de la mejor importancia
        mejor_importancia = gestiones_ordenadas["IMPORTANCIA"].iloc[0]
        gestiones_ordenadas = gestiones_ordenadas[gestiones_ordenadas["IMPORTANCIA"] == mejor_importancia]
        # Buscar la(s) gestión(es) con la fecha más cercana (máxima) anterior o igual a la fecha de pago
        fecha_max = gestiones_ordenadas["FECHA"].max()
        gestiones_mas_cercanas = gestiones_ordenadas[gestiones_ordenadas["FECHA"] == fecha_max]
        # Si hay varias gestiones de ese día, tomar la de menor hora
        gestion_asignada = gestiones_mas_cercanas.sort_values(
            by=["HORA_FORMATEADA"], ascending=[True]
        ).iloc[0]

        asignaciones.append(gestion_asignada["FUNCIONARIO"])
        fechas_asignadas.append(gestion_asignada["FECHA"].strftime('%Y-%m-%d'))
        resultados_asignados.append(gestion_asignada["NOMBRE_RESULTADO"])
        horas_asignadas.append(gestion_asignada["HORA_FORMATEADA"])


# Agregar resultados al DataFrame de pagos
df_pagos["FUNCIONARIO"] = asignaciones
df_pagos["FECHA_GESTION"] = fechas_asignadas
#df_pagos["HORA_GESTION"] = horas_asignadas
df_pagos["RESULTADO_GESTION"] = resultados_asignados

# Exportar resultados
df_pagos.to_excel(ruta_exporte, index=False)
print("✅ Proceso finalizado. Archivo exportado en:", ruta_exporte)
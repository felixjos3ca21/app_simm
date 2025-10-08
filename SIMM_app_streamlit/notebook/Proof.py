import os
import pandas as pd
import tkinter as tk
from tkinter import filedialog, messagebox
from tkinter import ttk

def procesar_archivos():
    # Pedir ruta de entrada
    messagebox.showinfo("Configuración", "Selecciona la carpeta donde están los archivos de entrada.")
    ruta_entrada = filedialog.askdirectory(title="Seleccionar carpeta de entrada")
    
    if not ruta_entrada:
        messagebox.showerror("Error", "La ruta de entrada no puede estar vacía.")
        return

    # Pedir ruta de salida
    messagebox.showinfo("Configuración", "Selecciona la carpeta donde se guardarán los archivos exportados.")
    ruta_salida = filedialog.askdirectory(title="Seleccionar carpeta de salida")
    
    if not ruta_salida:
        messagebox.showerror("Error", "La ruta de salida no puede estar vacía.")
        return

    # Verificar que las rutas existen
    if not os.path.exists(ruta_entrada):
        messagebox.showerror("Error", f"La ruta de entrada no existe: {ruta_entrada}")
        return
    if not os.path.exists(ruta_salida):
        messagebox.showerror("Error", f"La ruta de salida no existe: {ruta_salida}")
        return

    archivo_control = os.path.join(ruta_salida, "archivos_procesados.txt")

    archivos_procesados = set()
    if os.path.exists(archivo_control):
        with open(archivo_control, "r") as f:
            archivos_procesados = set(f.read().splitlines())

    nuevos_archivos = []
    dataframes = []

    # Barra de progreso
    progress_var = tk.DoubleVar()
    progress = ttk.Progressbar(ventana, variable=progress_var, maximum=100, length=300)
    progress.pack(pady=10)

    # Leer los archivos de la ruta de entrada
    archivos = [archivo for archivo in os.listdir(ruta_entrada) if archivo.endswith(".csv") or archivo.endswith(".xlsx")]
    total_archivos = len(archivos)
    
    for idx, archivo in enumerate(archivos):
        if archivo not in archivos_procesados:
            ruta_completa = os.path.join(ruta_entrada, archivo)
            try:
                df_temp = pd.read_excel(ruta_completa, engine="openpyxl")
                dataframes.append(df_temp)
                nuevos_archivos.append(archivo)
            except Exception as e:
                messagebox.showerror("Error", f"Error al leer el archivo {archivo}: {e}")
                continue

        # Actualizar progreso
        progress_var.set((idx + 1) / total_archivos * 100)
        ventana.update_idletasks()

    if not dataframes:
        messagebox.showinfo("Proceso finalizado", "No se encontraron archivos nuevos para procesar.")
        return

    df = pd.concat(dataframes, ignore_index=True)
    df["FECHA_REGISTRO"] = pd.to_datetime(df["FECHA_REGISTRO"])
    df["FECHA"] = df["FECHA_REGISTRO"].dt.strftime("%d-%m-%Y")
    df["HORA_REGISTRO"] = df["FECHA_REGISTRO"].dt.hour

    # Procesar y exportar base_numero_documento
    df_numero_documento = df.drop_duplicates(subset=["NUMERO_DOCUMENTO", "FECHA", "FUNCIONARIO"])[[
        "NUMERO_DOCUMENTO", "FECHA", "HORA_REGISTRO", "TIPOLLAMADA",
        "ID_REGISTRO_GESTION", "NOMBRE_RESULTADO", "FECHA_COMPROMISO_PAGO",
        "FUNCIONARIO", "NUMERO_OBLIGACION", "NRO_COMPARENDO"
    ]]
    df_numero_documento.to_csv(os.path.join(ruta_salida, "base_numero_documento.csv"), index=False, encoding="utf-8")

    # Procesar y exportar base_nro_comparendo
    df_nro_comparendo = df.drop_duplicates(subset=["NRO_COMPARENDO", "FECHA", "FUNCIONARIO"])[[
        "NRO_COMPARENDO", "FECHA", "HORA_REGISTRO", "TIPOLLAMADA",
        "ID_REGISTRO_GESTION", "NOMBRE_RESULTADO", "FECHA_COMPROMISO_PAGO",
        "FUNCIONARIO", "NUMERO_OBLIGACION", "NUMERO_DOCUMENTO"
    ]]
    df_nro_comparendo.to_csv(os.path.join(ruta_salida, "base_nro_comparendo.csv"), index=False, encoding="utf-8")

    # Procesar y exportar base_id_gestion
    df_id_gestion = df.drop_duplicates(subset=["ID_REGISTRO_GESTION", "FECHA", "FUNCIONARIO"])[[
        "ID_REGISTRO_GESTION", "FECHA", "HORA_REGISTRO", "TIPOLLAMADA",
        "NUMERO_DOCUMENTO", "NOMBRE_RESULTADO", "FECHA_COMPROMISO_PAGO",
        "FUNCIONARIO", "NUMERO_OBLIGACION", "NRO_COMPARENDO","TIPO_CHAT"
    ]]
    df_id_gestion.to_csv(os.path.join(ruta_salida, "base_id_gestion.csv"), index=False, encoding="utf-8")

    # Exportar base_acuerdos_pago
    df_acuerdos_pago = df_id_gestion[df_id_gestion["NOMBRE_RESULTADO"].isin([
        "Compromiso de acuerdo de pago", "Compromiso de pago"
    ])]
    df_acuerdos_pago = df_acuerdos_pago[[
        "NUMERO_DOCUMENTO", "FECHA", "HORA_REGISTRO", "TIPOLLAMADA", 
        "ID_REGISTRO_GESTION", "NOMBRE_RESULTADO", "FECHA_COMPROMISO_PAGO", 
        "FUNCIONARIO", "NUMERO_OBLIGACION", "NRO_COMPARENDO"
    ]]
    df_acuerdos_pago.to_csv(os.path.join(ruta_salida, "base_acuerdos_pago.csv"), index=False, encoding="utf-8")

    # Generar base_gestion_asesores con los valores correctos
    asesores = []
    for funcionario in df["FUNCIONARIO"].unique():
        for fecha in df["FECHA"].unique():
            # Filtrar datos por funcionario y fecha
            df_funcionario_fecha = df[df["FUNCIONARIO"] == funcionario]
            df_funcionario_fecha = df_funcionario_fecha[df_funcionario_fecha["FECHA"] == fecha]
            
            cantidad_numero_documento = df_funcionario_fecha["NUMERO_DOCUMENTO"].nunique()
            cantidad_id_registro_gestion = df_funcionario_fecha["ID_REGISTRO_GESTION"].nunique()
            cantidad_nro_comparendo = df_funcionario_fecha["NRO_COMPARENDO"].nunique()
            cantidad_acuerdos_pago = df_funcionario_fecha[df_funcionario_fecha["NOMBRE_RESULTADO"].isin([
                "Compromiso de acuerdo de pago", "Compromiso de pago"
            ])].shape[0]

            # Agregar fila con los resultados
            asesores.append({
                "FUNCIONARIO": funcionario,
                "FECHA": fecha,
                "CANTIDAD_NUMERO_DOCUMENTO": cantidad_numero_documento,
                "CANTIDAD_ID_REGISTRO_GESTION": cantidad_id_registro_gestion,
                "CANTIDAD_NRO_COMPARENDO": cantidad_nro_comparendo,
                "CANTIDAD_ACUERDOS_DE_PAGO": cantidad_acuerdos_pago
            })

    # Crear dataframe con la información de los asesores
    df_asesores = pd.DataFrame(asesores)
    df_asesores.to_csv(os.path.join(ruta_salida, "base_gestion_asesores.csv"), index=False, encoding="utf-8")

    # Actualizar el archivo de control con los nuevos archivos procesados
    with open(archivo_control, "a") as f:
        f.write("\n".join(nuevos_archivos) + "\n")

    messagebox.showinfo("Proceso finalizado", f"Se procesaron {len(nuevos_archivos)} archivos:\n" + "\n".join(nuevos_archivos))

# Interfaz gráfica
ventana = tk.Tk()
ventana.title("Procesador de Gestiones")
ventana.geometry("400x300")

label = tk.Label(ventana, text="Haz clic en el botón para procesar los archivos.", font=("Arial", 12))
label.pack(pady=10)

boton_procesar = tk.Button(ventana, text="Procesar Archivos", font=("Arial", 12), command=procesar_archivos)
boton_procesar.pack(pady=20)

ventana.mainloop()
from sqlalchemy import create_engine, text

# Configuración de conexión
DB_CONFIG = {
    'host': 'localhost',
    'database': 'SIMM',
    'user': 'postgres',
    'password': 'admin',
    'port': '5432'
}

def crear_tabla_SMS():
    try:
        # Conexión a PostgreSQL
        engine = create_engine(
            f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
        )

        # Script SQL corregido
        sql_script = """
        CREATE TABLE IF NOT EXISTS SMS (
            id_registro VARCHAR(64) PRIMARY KEY,          -- ID hash SHA-256 (64 caracteres)
            tipo_documento VARCHAR(50),                   -- "Tipo documento"
            documento VARCHAR(30),                        -- "Número documento"
            nombre_usuario VARCHAR(100),                  -- "Nombre"
            fecha_SMS DATE NOT NULL,                      -- "Fecha envio de SMS"
            resultado VARCHAR(100),                       -- "Resultado"
            texto_sms VARCHAR(250),                       -- "Fecha Compromiso"
            base VARCHAR(50),                             -- "Funcionario"
            telefono VARCHAR(20) NOT NULL,                -- "Teléfono"
            numero_comparendo VARCHAR(50),                -- "Nro. Comparendo"
            identificador_infraccion VARCHAR(50),         -- Campo calculado
            archivo_origen VARCHAR(100),                  -- Nombre del archivo fuente
            fecha_carga TIMESTAMP DEFAULT CURRENT_TIMESTAMP -- Fecha de carga automática
        );"""

        # Ejecutar el script
        with engine.connect() as conn:
            conn.execute(text(sql_script))
            conn.commit()

        print("✅ Tabla 'SMS' creada exitosamente")

    except Exception as e:
        print(f"❌ Error al crear la tabla: {e}")

if __name__ == "__main__":
    crear_tabla_SMS()
from sqlalchemy import create_engine, text

# Configuración de conexión (ajusta según tu entorno)
DB_CONFIG = {
    'host': 'localhost',
    'database': 'SIMM',
    'user': 'postgres',
    'password': 'admin',
    'port': '5432'
}

def crear_tabla_bases():
    try:
        # Conexión a PostgreSQL
        engine = create_engine(
            f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
        )

        # Script SQL para la tabla
        sql_script = """
        CREATE TABLE IF NOT EXISTS bases (
            id_registro VARCHAR(75),         -- ID hash generado (SHA-256)
            base VARCHAR(100) NOT NULL,                   -- "BASE"
            fecha_entrega DATE NOT NULL,            -- "FECHA DE ENTREGA"
            tipo_documento VARCHAR(50),                  -- "TIPO DE DCTO"
            documento VARCHAR(50),         -- "IDENTIFICACIÓN"
            nombre VARCHAR(200),                        -- "NOMBRE"
            numero_comparendo VARCHAR(75),       -- "NRO. COMPARENDO"
            fecha_comparendo DATE,                      -- "FECHA DE COMPARENDO"
            codigo_infraccion VARCHAR(20),               -- "CODIGO DE INFRACCIÓN"
            placa VARCHAR(20),                          -- "PLACA"
            valor_infraccion NUMERIC(16, 2),            -- "VALOR INFRACCIÓN"
            telefono VARCHAR(30),              -- "CEL1"
            valor_intereses NUMERIC(12, 2),              -- "VALOR INTERESES"
            
            archivo_origen VARCHAR(100) NOT NULL,        -- Nombre del archivo fuente
            fecha_carga TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );"""

        # Ejecutar el script
        with engine.connect() as conn:
            # Eliminar tabla si ya existe (opcional, solo para desarrollo)
            conn.execute(text("DROP TABLE IF EXISTS bases CASCADE"))
            
            # Crear nueva tabla
            conn.execute(text(sql_script))
            conn.commit()

        print("✅ Tabla 'BASES' creada exitosamente")

    except Exception as e:
        print(f"❌ Error al crear la tabla: {e}")

if __name__ == "__main__":
    crear_tabla_bases()
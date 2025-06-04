DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 
        FROM information_schema.columns 
        WHERE table_name = 'bases' AND column_name = 'estado_telefono'
    ) THEN
        ALTER TABLE bases ADD COLUMN estado_telefono VARCHAR(150);
    END IF;
END $$;


-- Paso 2: Crear función de actualización
CREATE OR REPLACE FUNCTION actualizar_estado_telefonos()
RETURNS TRIGGER AS $$
BEGIN
    -- Actualizar el registro actual
    NEW.estado_telefono = CASE
        -- 1. Teléfonos ausentes
        WHEN NEW.telefono IS NULL THEN 'AUSENTE: Valor NULL'
        WHEN TRIM(NEW.telefono) = '' THEN 'AUSENTE: Cadena vacía'

        -- 2. Teléfonos con solo ceros
        WHEN REGEXP_REPLACE(NEW.telefono, '^\+57|[^0-9]', '', 'g') ~ '^0+$' THEN 
            CASE 
                WHEN LENGTH(REGEXP_REPLACE(NEW.telefono, '^\+57|[^0-9]', '', 'g')) = 1 THEN 'ERRADO: Un solo cero'
                ELSE 'ERRADO: ' || LENGTH(REGEXP_REPLACE(NEW.telefono, '^\+57|[^0-9]', '', 'g')) || ' ceros consecutivos'
            END

        -- 3. Longitudes inválidas
        WHEN LENGTH(REGEXP_REPLACE(NEW.telefono, '^\+57|[^0-9]', '', 'g')) NOT IN (7, 10) 
            THEN 'ERRADO: Longitud incorrecta (' || LENGTH(REGEXP_REPLACE(NEW.telefono, '^\+57|[^0-9]', '', 'g')) || ' dígitos)'

        -- 4. Secuencias obvias
        WHEN REGEXP_REPLACE(NEW.telefono, '^\+57|[^0-9]', '', 'g') ~ '^(1234567|7654321|9876543|3210987|0123456|5432109|1234567890|9876543210|0123456789)' 
            THEN 'ERRADO: Secuencia numérica obvia'

        -- 5. Patrones repetitivos
        WHEN REGEXP_REPLACE(NEW.telefono, '^\+57|[^0-9]', '', 'g') ~ '^(\d)\1+$' THEN 'ERRADO: Todos los dígitos iguales'
        WHEN REGEXP_REPLACE(NEW.telefono, '^\+57|[^0-9]', '', 'g') ~ '(\d)\1{4}' THEN 'ERRADO: Demasiadas repeticiones (4+ dígitos iguales)'
        WHEN REGEXP_REPLACE(NEW.telefono, '^\+57|[^0-9]', '', 'g') ~ '^(\d\d)\1+' THEN 'ERRADO: Patrón repetitivo'
        WHEN LENGTH(REGEXP_REPLACE(NEW.telefono, '^\+57|[^0-9]', '', 'g')) = 7 
             AND REGEXP_REPLACE(NEW.telefono, '^\+57|[^0-9]', '', 'g') ~ '^(0{5,}|1{5,}|2{5,}|9{5,})$' 
            THEN 'ERRADO: Fijo con patrón inválido (repetitivo)'

        -- 6. Validación fijo regional (7 dígitos)
        WHEN LENGTH(REGEXP_REPLACE(NEW.telefono, '^\+57|[^0-9]', '', 'g')) = 7 
            THEN 'VALIDO: Fijo regional'

        -- 7. Validación móvil con patrón inválido
        WHEN REGEXP_REPLACE(NEW.telefono, '^\+57|[^0-9]', '', 'g') ~ '^3\d{2}(0{5,}|1{5,}|2{5,}|6{5,}|9{5,})$' 
            THEN 'ERRADO: Móvil con patrón inválido (repetitivo)'

        -- 8. Validación móvil válido
        WHEN LENGTH(REGEXP_REPLACE(NEW.telefono, '^\+57|[^0-9]', '', 'g')) = 10 
             AND REGEXP_REPLACE(NEW.telefono, '^\+57|[^0-9]', '', 'g') ~ '^3\d{9}$' 
            THEN 'VALIDO: Móvil'

        -- 9. Fijos con indicativo
        WHEN LENGTH(REGEXP_REPLACE(NEW.telefono, '^\+57|[^0-9]', '', 'g')) = 10 
             AND LEFT(REGEXP_REPLACE(NEW.telefono, '^\+57|[^0-9]', '', 'g'), 3) IN ('601','604','605','607','608') 
            THEN 'VALIDO: Teléfono fijo largo (' || LEFT(REGEXP_REPLACE(NEW.telefono, '^\+57|[^0-9]', '', 'g'), 3) || ')'

        -- 10. Internacionales
        WHEN NEW.telefono ~ '^\+57' 
             AND REGEXP_REPLACE(NEW.telefono, '^\+57|[^0-9]', '', 'g') ~ '^3\d{9}$' 
            THEN 'VALIDO: Móvil con código país'

        -- 11. Demasiados ceros
        WHEN (LENGTH(REGEXP_REPLACE(NEW.telefono, '^\+57|[^0-9]', '', 'g')) - 
             LENGTH(REGEXP_REPLACE(REGEXP_REPLACE(NEW.telefono, '^\+57|[^0-9]', '', 'g'), '0', '', 'g'))) > 
             (LENGTH(REGEXP_REPLACE(NEW.telefono, '^\+57|[^0-9]', '', 'g'))/2) 
            THEN 'ERRADO: Demasiados ceros'

        -- 12. Por defecto
        ELSE 'ERRADO: No cumple ningún patrón válido'
    END;
    
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trigger_actualizar_estado_telefono ON bases;


-- Paso 3: Crear trigger para actualización automática
CREATE TRIGGER trigger_actualizar_estado_telefono
    BEFORE INSERT OR UPDATE OF telefono ON bases
    FOR EACH ROW
    EXECUTE FUNCTION actualizar_estado_telefonos();

-- Paso 4: Ejecutar primera actualización masiva
-- Paso 4: Actualizar registros existentes (ejecutar solo una vez)
UPDATE bases 
SET estado_telefono = CASE
    -- 1. Teléfonos ausentes
    WHEN telefono IS NULL THEN 'AUSENTE: Valor NULL'
    WHEN TRIM(telefono) = '' THEN 'AUSENTE: Cadena vacía'

    -- 2. Teléfonos con solo ceros
    WHEN REGEXP_REPLACE(telefono, '^\+57|[^0-9]', '', 'g') ~ '^0+$' THEN 
        CASE 
            WHEN LENGTH(REGEXP_REPLACE(telefono, '^\+57|[^0-9]', '', 'g')) = 1 THEN 'ERRADO: Un solo cero'
            ELSE 'ERRADO: ' || LENGTH(REGEXP_REPLACE(telefono, '^\+57|[^0-9]', '', 'g')) || ' ceros consecutivos'
        END

    -- 3. Longitudes inválidas
    WHEN LENGTH(REGEXP_REPLACE(telefono, '^\+57|[^0-9]', '', 'g')) NOT IN (7, 10) 
        THEN 'ERRADO: Longitud incorrecta (' || LENGTH(REGEXP_REPLACE(telefono, '^\+57|[^0-9]', '', 'g')) || ' dígitos)'

    -- 4. Secuencias obvias
    WHEN REGEXP_REPLACE(telefono, '^\+57|[^0-9]', '', 'g') ~ '^(1234567|7654321|9876543|3210987|0123456|5432109|1234567890|9876543210|0123456789)' 
        THEN 'ERRADO: Secuencia numérica obvia'

    -- 5. Patrones repetitivos
    WHEN REGEXP_REPLACE(telefono, '^\+57|[^0-9]', '', 'g') ~ '^(\d)\1+$' THEN 'ERRADO: Todos los dígitos iguales'
    WHEN REGEXP_REPLACE(telefono, '^\+57|[^0-9]', '', 'g') ~ '(\d)\1{4}' THEN 'ERRADO: Demasiadas repeticiones (4+ dígitos iguales)'
    WHEN REGEXP_REPLACE(telefono, '^\+57|[^0-9]', '', 'g') ~ '^(\d\d)\1+' THEN 'ERRADO: Patrón repetitivo'
    WHEN LENGTH(REGEXP_REPLACE(telefono, '^\+57|[^0-9]', '', 'g')) = 7 
         AND REGEXP_REPLACE(telefono, '^\+57|[^0-9]', '', 'g') ~ '^(0{5,}|1{5,}|2{5,}|9{5,})$' 
        THEN 'ERRADO: Fijo con patrón inválido (repetitivo)'

    -- 6. Validación fijo regional (7 dígitos)
    WHEN LENGTH(REGEXP_REPLACE(telefono, '^\+57|[^0-9]', '', 'g')) = 7 
        THEN 'VALIDO: Fijo regional'

    -- 7. Validación móvil con patrón inválido
    WHEN REGEXP_REPLACE(telefono, '^\+57|[^0-9]', '', 'g') ~ '^3\d{2}(0{5,}|1{5,}|2{5,}|6{5,}|9{5,})$' 
        THEN 'ERRADO: Móvil con patrón inválido (repetitivo)'

    -- 8. Validación móvil válido
    WHEN LENGTH(REGEXP_REPLACE(telefono, '^\+57|[^0-9]', '', 'g')) = 10 
         AND REGEXP_REPLACE(telefono, '^\+57|[^0-9]', '', 'g') ~ '^3\d{9}$' 
        THEN 'VALIDO: Móvil'

    -- 9. Fijos con indicativo
    WHEN LENGTH(REGEXP_REPLACE(telefono, '^\+57|[^0-9]', '', 'g')) = 10 
         AND LEFT(REGEXP_REPLACE(telefono, '^\+57|[^0-9]', '', 'g'), 3) IN ('601','604','605','607','608') 
        THEN 'VALIDO: Teléfono fijo largo (' || LEFT(REGEXP_REPLACE(telefono, '^\+57|[^0-9]', '', 'g'), 3) || ')'

    -- 10. Internacionales
    WHEN telefono ~ '^\+57' 
         AND REGEXP_REPLACE(telefono, '^\+57|[^0-9]', '', 'g') ~ '^3\d{9}$' 
        THEN 'VALIDO: Móvil con código país'

    -- 11. Demasiados ceros
    WHEN (LENGTH(REGEXP_REPLACE(telefono, '^\+57|[^0-9]', '', 'g')) - 
         LENGTH(REGEXP_REPLACE(REGEXP_REPLACE(telefono, '^\+57|[^0-9]', '', 'g'), '0', '', 'g'))) > 
         (LENGTH(REGEXP_REPLACE(telefono, '^\+57|[^0-9]', '', 'g'))/2) 
        THEN 'ERRADO: Demasiados ceros'

    -- 12. Por defecto
    ELSE 'ERRADO: No cumple ningún patrón válido'
END
WHERE estado_telefono IS NULL OR estado_telefono = '';

-- Verificar resultados
SELECT estado_telefono, COUNT(*) as cantidad
FROM bases 
GROUP BY estado_telefono 
ORDER BY cantidad DESC;

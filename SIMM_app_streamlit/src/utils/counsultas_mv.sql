-- ============================================
-- PASO 1: CREAR LOS ÍNDICES PARA LA VISTA MATERIALIZADA
-- ============================================

-- Índice principal en id_registro (clave primaria lógica)
CREATE UNIQUE INDEX idx_mv_pagos_relacionados_id_registro 
ON mv_pagos_relacionados (id_registro);

-- Índices para búsquedas por comparendo y acuerdo
CREATE INDEX idx_mv_pagos_relacionados_nro_comparendo 
ON mv_pagos_relacionados (nro_comparendo) 
WHERE nro_comparendo IS NOT NULL;

CREATE INDEX idx_mv_pagos_relacionados_nro_acuerdo 
ON mv_pagos_relacionados (nro_acuerdo) 
WHERE nro_acuerdo IS NOT NULL;

-- Índice para búsquedas por documento
CREATE INDEX idx_mv_pagos_relacionados_documento 
ON mv_pagos_relacionados (documento);

-- Índice compuesto para búsquedas por documento + comparendo/acuerdo
CREATE INDEX idx_mv_pagos_relacionados_doc_comp 
ON mv_pagos_relacionados (documento, nro_comparendo);

CREATE INDEX idx_mv_pagos_relacionados_doc_acuerdo 
ON mv_pagos_relacionados (documento, nro_acuerdo);

-- Índice para búsquedas por fecha de pago
CREATE INDEX idx_mv_pagos_relacionados_fecha_pago 
ON mv_pagos_relacionados (fecha_pago);

-- Índice para búsquedas por rango de fechas y documento
CREATE INDEX idx_mv_pagos_relacionados_fecha_doc 
ON mv_pagos_relacionados (fecha_pago, documento);

-- Índices para filtros por flags booleanos
CREATE INDEX idx_mv_pagos_relacionados_tiene_gestiones 
ON mv_pagos_relacionados (tiene_gestiones) 
WHERE tiene_gestiones = TRUE;

CREATE INDEX idx_mv_pagos_relacionados_tiene_sms 
ON mv_pagos_relacionados (tiene_sms) 
WHERE tiene_sms = TRUE;

-- Índice para ordenamiento por valor
CREATE INDEX idx_mv_pagos_relacionados_valor 
ON mv_pagos_relacionados (valor DESC);

-- Índice compuesto para reportes comunes (fecha + valor)
CREATE INDEX idx_mv_pagos_relacionados_fecha_valor 
ON mv_pagos_relacionados (fecha_pago DESC, valor DESC);

-- ============================================
-- PASO 2: REALIZAR EL REFRESH DE LA VISTA MATERIALIZADA
-- ============================================

-- Opción 1: REFRESH COMPLETO (recomendado para primera vez)
--REFRESH MATERIALIZED VIEW mv_pagos_relacionados;

-- Opción 2: REFRESH CONCURRENTE (sin bloqueo, pero requiere índice único)
--REFRESH MATERIALIZED VIEW CONCURRENTLY mv_pagos_relacionados;

-- ============================================
-- COMANDOS ADICIONALES ÚTILES
-- ============================================

-- Ver el tamaño de la vista materializada
SELECT 
    schemaname,
    matviewname,
    pg_size_pretty(pg_total_relation_size(schemaname||'.'||matviewname)) as size
FROM pg_matviews 
WHERE matviewname = 'mv_pagos_relacionados';

-- Ver los índices creados
SELECT 
    indexname,
    indexdef,
    pg_size_pretty(pg_relation_size(indexname::regclass)) as index_size
FROM pg_indexes 
WHERE tablename = 'mv_pagos_relacionados'
ORDER BY indexname;

-- Ver estadísticas de la vista
SELECT 
    COUNT(*) as total_registros,
    COUNT(DISTINCT id_registro) as registros_unicos,
    COUNT(*) FILTER (WHERE tiene_gestiones = TRUE) as con_gestiones,
    COUNT(*) FILTER (WHERE tiene_sms = TRUE) as con_sms,
    MIN(fecha_pago) as fecha_pago_min,
    MAX(fecha_pago) as fecha_pago_max
FROM mv_pagos_relacionados;

-- ============================================
-- SCRIPT PARA AUTOMATIZAR EL REFRESH PERIÓDICO
-- ============================================

-- Si quieres crear un procedimiento para refresh automático:
CREATE OR REPLACE FUNCTION refresh_mv_pagos_relacionados()
RETURNS void AS $$
BEGIN
    -- Log del inicio
    RAISE NOTICE 'Iniciando refresh de mv_pagos_relacionados a las: %', NOW();
    
    -- Refresh concurrente si es posible, sino normal
    BEGIN
        REFRESH MATERIALIZED VIEW CONCURRENTLY mv_pagos_relacionados;
        RAISE NOTICE 'Refresh concurrente completado exitosamente';
    EXCEPTION WHEN OTHERS THEN
        RAISE NOTICE 'Refresh concurrente falló, intentando refresh normal: %', SQLERRM;
        REFRESH MATERIALIZED VIEW mv_pagos_relacionados;
        RAISE NOTICE 'Refresh normal completado';
    END;
    
    -- Actualizar estadísticas
    ANALYZE mv_pagos_relacionados;
    
    RAISE NOTICE 'Refresh completado a las: %', NOW();
END;
$$ LANGUAGE plpgsql;

----------------------------------------------------------------------------------------------


Select REPLACE(to_char(sum(valor), 'FM$999,999,999,990.00'), ',', '.') AS Recaudo_Andes
FROM mv_pagos_relacionados
WHERE 
fecha_pago >= '2025-04-01'::DATE
and fecha_pago < '2025-05-01'::DATE;

SELECT 
    REPLACE(to_char(sum(valor), 'FM$999,999,999,990.00'), ',', '.') AS Recaudo_Total,
    REPLACE(to_char(sum(CASE WHEN tiene_sms = TRUE AND tiene_gestiones = TRUE THEN valor ELSE 0 END), 'FM$999,999,999,990.00'), ',', '.') AS Recaudo_SMS_y_Gestion,
    REPLACE(to_char(sum(CASE WHEN tiene_sms = TRUE AND tiene_gestiones = FALSE THEN valor ELSE 0 END), 'FM$999,999,999,990.00'), ',', '.') AS Recaudo_Solo_SMS,
    REPLACE(to_char(sum(CASE WHEN tiene_sms = FALSE AND tiene_gestiones = TRUE THEN valor ELSE 0 END), 'FM$999,999,999,990.00'), ',', '.') AS Recaudo_Solo_Gestion,
    COUNT(*) AS Total_Pagos
FROM mv_pagos_relacionados
WHERE 
    fecha_pago >= '2025-04-01'::DATE
    AND fecha_pago < '2025-05-01'::DATE
    AND (tiene_sms = TRUE OR tiene_gestiones = TRUE);

SELECT 
    COUNT(*) as total_registros,
    COUNT(DISTINCT id_registro) as registros_unicos,
    CASE 
        WHEN COUNT(*) = COUNT(DISTINCT id_registro) THEN 'Sin duplicados ✓'
        ELSE 'HAY DUPLICADOS ⚠️'
    END as estado_duplicados
FROM mv_pagos_relacionados
WHERE 
    fecha_pago >= '2025-04-01'::DATE
    AND fecha_pago < '2025-05-01'::DATE
    AND (tiene_sms = TRUE OR tiene_gestiones = TRUE);


select distinct* from pagos;

SELECT DISTINCT ON (id_registro) *
FROM pagos
ORDER BY id_registro, fecha_carga DESC;

SELECT *
FROM (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY id_registro ORDER BY fecha_carga DESC) AS rn
    FROM pagos
) t
WHERE rn = 1
and fecha_pago >= '2025-01-01':: DATE;  -- Solo la fila más reciente de cada grupo



select * from pagos
where identificador_infraccion = '456340';



REFRESH MATERIALIZED VIEW mv_gestiones_comparendo;
REFRESH MATERIALIZED VIEW mv_sms_comparendos;
REFRESH MATERIALIZED VIEW mv_gestiones_comparendo;


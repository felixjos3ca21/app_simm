SELECT * from bases limit 10;



SELECT sum(valor_infraccion) from bases
WHERE fecha_entrega >= '2025-08-01';


Select base, count(*) as cantidad from bases
where fecha_entrega >= '2025-08-01'
group by base 
order by cantidad desc;

SELECT base, fecha_entrega, COUNT(*) AS cantidad
FROM bases
WHERE fecha_entrega >= '2025-08-01'
GROUP BY base, fecha_entrega
ORDER BY base, fecha_entrega;





select estado_telefono, count(*) as cantidad from bases
where fecha_entrega >= '2025-08-01'
group by estado_telefono
ORDER by cantidad desc;


SELECT base, count(*) as cantidad from bases
where estado_telefono = 'AUSENTE: Cadena vacía'
and fecha_entrega >= '2025-08-01'
group by base
order by cantidad DESC;


SELECT 
    t.base,
    t.total_registros,
    COALESCE(s.sin_telefono, 0) AS sin_telefono,
    ROUND(100.0 * COALESCE(s.sin_telefono, 0) / t.total_registros, 2) AS porcentaje_sin_telefono
FROM (
    SELECT base, COUNT(*) AS total_registros
    FROM bases
    WHERE fecha_entrega >= '2025-08-01'
    GROUP BY base
) t
LEFT JOIN (
    SELECT base, COUNT(*) AS sin_telefono
    FROM bases
    WHERE estado_telefono = 'AUSENTE: Cadena vacía'
      AND fecha_entrega >= '2025-08-01'
    GROUP BY base
) s
ON t.base = s.base
ORDER BY porcentaje_sin_telefono DESC;


SELECT 
    base,
    COUNT(*) AS total_registros,

    -- Ausentes (NULL o vacío detectado por tu trigger)
    COUNT(*) FILTER (WHERE estado_telefono LIKE 'AUSENTE:%') AS ausentes,

    -- Errados (cualquier variante de ERRADO)
    COUNT(*) FILTER (WHERE estado_telefono LIKE 'ERRADO:%') AS errados,

    -- Válidos (cualquier variante de VALIDO)
    COUNT(*) FILTER (WHERE estado_telefono LIKE 'VALIDO:%') AS validos,

    -- Porcentajes
    ROUND(100.0 * COUNT(*) FILTER (WHERE estado_telefono LIKE 'AUSENTE:%') / COUNT(*), 2) AS pct_ausentes,
    ROUND(100.0 * COUNT(*) FILTER (WHERE estado_telefono LIKE 'ERRADO:%') / COUNT(*), 2) AS pct_errados,
    ROUND(100.0 * COUNT(*) FILTER (WHERE estado_telefono LIKE 'VALIDO:%') / COUNT(*), 2) AS pct_validos

FROM bases
WHERE fecha_entrega >= '2025-08-01'
GROUP BY base
ORDER BY pct_validos;


ANALYSE sms;
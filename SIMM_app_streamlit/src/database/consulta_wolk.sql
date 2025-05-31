select * from resultados_predictivo
limit 10;

select telephone from campanas_3
where module = 'andes-movilidadtigo'
limit 10;

select telephone from cdr_5
where module = 'andes-movilidadtigo'
limit 10;

select movil from movilidad
limit 10;

SELECT * FROM CAMPANAS_3
LIMIT 10
;

SELECT 
DATE(fechafinreg) AS fecha,
SUM(llamadas) AS llamadas_predictivas
FROM resultados_predictivo
WHERE fechafinreg BETWEEN '2025-05-01'::DATE AND '2025-05-04'::DATE
GROUP BY DATE(fechafinreg)
ORDER BY fecha DESC;
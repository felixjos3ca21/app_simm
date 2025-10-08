SELECT max(date) from tipificaciones_3
SELECT max(date) from cdr_5
SELECT max(date) from campanas_3
SELECT max(fecha_sms) from sms

SELECT distinct(tipificaciones_3.type_interaction) from tipificaciones_3

SELECT count(*) from tipificaciones_3
WHERE tipificaciones_3.type_interaction = 'out_pre'
AND date >= '2025-09-01'
	and date <= '2025-09-30'

out_pre

SELECT count(*) from cdr_5
where date >= '2025-09-01'
	and date <= '2025-09-30'

SELECT count(*) from sms
where sms.fecha_sms >= '2025-09-01'
	and sms.fecha_sms <= '2025-09-30'



Select min(bases.fecha_entrega) from bases
Where bases.fecha_entrega >= '2025-09-01'
	and bases.fecha_entrega <= '2025-09-30'


select base, count(*) from bases 
Where bases.fecha_entrega >= '2025-09-01'
	and bases.fecha_entrega <= '2025-09-30'
group by base


select * from pagos limit 10

select sum(valorpago) from pagos
where aplicacion_final = 'APLICA'
	and fechapago >= '2025-08-21'
	and fechapago <= '2025-09-20'


Select count(*) from gestiones
where gestiones.fecha_gestion_sencilla >= '2025-10-01'

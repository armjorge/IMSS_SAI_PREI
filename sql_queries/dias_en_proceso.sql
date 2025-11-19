SELECT 
    dias_en_proceso::text AS dias_en_proceso,
    SUM(importe)::numeric(12,2) AS total_importe
FROM eseotres_warehouse.cpi_dias_en_proceso
WHERE estado_c_r_ = 'En proceso'
GROUP BY dias_en_proceso::text
ORDER BY dias_en_proceso::text;

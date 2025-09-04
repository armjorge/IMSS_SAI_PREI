SELECT 
    CASE 
        WHEN "estado_c.r." IS NULL THEN 'Grand Total'
        ELSE "estado_c.r."
    END AS estado,
    TO_CHAR(SUM(importe), 'FM$999,999,999,990.00') AS total_importe
FROM eseotres_warehouse.altas_historicas
WHERE 
    file_date = (SELECT MAX(file_date) FROM eseotres_warehouse.altas_historicas)
    AND fechaaltatrunc::date >= '2025-06-30'::date
GROUP BY 
    ROLLUP("estado_c.r.")
ORDER BY 
    SUM(importe) ASC;
SELECT 
    CASE 
        WHEN "estado_c.r." IS NULL THEN 'Grand Total'
        ELSE "estado_c.r."
    END AS estado,
    TO_CHAR(SUM(importe), 'FM$999,999,999,990.00') AS total_importe
FROM eseotres.df_altas
GROUP BY 
    ROLLUP("estado_c.r.")
ORDER BY 
    SUM(importe) DESC;
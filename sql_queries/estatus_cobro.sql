SELECT 
    "estado_c.r." AS estado,
        TO_CHAR(SUM(importe), 'FM$999,999,999,990.00') AS total_importe
FROM eseotres.df_altas
GROUP BY 
    "estado_c.r."
    
ORDER BY 
    total_importe DESC;

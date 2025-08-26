SELECT 
    CASE 
        WHEN DATE_TRUNC('month', fechaaltatrunc) IS NULL THEN 'Grand Total'
        ELSE TO_CHAR(DATE_TRUNC('month', fechaaltatrunc), 'Mon YYYY')
    END AS mes_del_alta,
    CASE 
        WHEN descunidad IS NULL AND DATE_TRUNC('month', fechaaltatrunc) IS NULL THEN ''
        WHEN descunidad IS NULL AND DATE_TRUNC('month', fechaaltatrunc) IS NOT NULL THEN 'Subtotal del Mes'
        ELSE descunidad
    END AS unidad_operativa,
    COUNT(*) AS num_registros,
    TO_CHAR(SUM(importe), 'FM$999,999,999,990.00') AS total_importe
FROM (
    SELECT 
        fechaaltatrunc,
        importe,
        descunidad
    FROM eseotres.df_altas
    WHERE 
        uuid != 'No localizado'
        AND "estado_c.r." IN ('Sin Contra Recibo', 'No localizado')
) subquery
GROUP BY 
    ROLLUP(DATE_TRUNC('month', fechaaltatrunc), descunidad)
ORDER BY 
    DATE_TRUNC('month', fechaaltatrunc) ASC NULLS LAST,
    descunidad ASC NULLS LAST;
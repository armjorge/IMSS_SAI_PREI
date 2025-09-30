SELECT
    CASE
        WHEN fechaaltatrunc = '2025-06-30' THEN '2025-06-30'
        ELSE 'after_2025-06-30'
    END AS grupo_fecha,
    "estado_c_r_" AS estado,
    CONCAT('$', CAST(SUM(importe) AS TEXT)) AS total_importe
FROM eseotres_warehouse.altas_historicas
WHERE file_date = (SELECT MAX(file_date) FROM eseotres_warehouse.altas_historicas)
    AND (fechaaltatrunc = '2025-06-30' OR fechaaltatrunc > '2025-06-30')
GROUP BY
    CASE
        WHEN fechaaltatrunc = '2025-06-30' THEN '2025-06-30'
        ELSE 'after_2025-06-30'
    END,
    "estado_c_r_"
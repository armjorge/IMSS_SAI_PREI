SELECT 
    cvearticulo,
    estatus,
    SUM(cantidadsolicitada) AS total_cantidadsolicitada
FROM (
    SELECT *
    FROM eseotres_warehouse.ordenes_historicas
    WHERE file_date = (
        SELECT MAX(file_date)
        FROM eseotres_warehouse.ordenes_historicas
    )
) AS latest_table
WHERE estatus = 'Pendiente'
GROUP BY 
    cvearticulo, 
    estatus
ORDER BY 
    estatus, 
    cvearticulo;

SELECT 
    estatus,
    cveArticulo,
    SUM(cantidadsancionable) AS total_sancionable
FROM eseotres_warehouse.ordenes_y_altas
WHERE file_date = (
    SELECT MAX(file_date)
    FROM eseotres_warehouse.ordenes_y_altas
)
  AND cantrecibida = 0
GROUP BY estatus, cveArticulo
ORDER BY estatus;
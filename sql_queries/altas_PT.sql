SELECT noalta, estado_c_r_, importe
FROM eseotres_warehouse.altas_historicas
WHERE file_date = (SELECT MAX(file_date) FROM eseotres_warehouse.altas_historicas)
AND noalta IN ('371902-805232', '361001-802964', '141901-801577', '271901-802429')
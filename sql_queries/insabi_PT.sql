SELECT numero_orden_suministro, 
estado_de_la_factura, importe
FROM eseotres_warehouse.imssb_historico
WHERE file_date = (SELECT MAX(file_date) FROM eseotres_warehouse.imssb_historico)
AND numero_orden_suministro IN ('IMB-23-02-2024-23017985-ASF', 'IMB-02-02-2024-02017989-F7', 'IMB-20-02-2024-20018995-ASF', 'IMB-25-02-2024-25018016-F7', 'IMB-16-02-2024-16018005-F7', 'IMB-09-02-2024-0901047689-F', 'IMB-12-02-2024-1201082531-ASF', 'IMB-17-02-2024-17134443-ASF', 'IMB-20-02-2024-2001090907-ASF', 'IMB-17-02-2024-1701086311-ASF')

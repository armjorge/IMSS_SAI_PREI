-- PostgreSQL query to select all from the df_altas table
SELECT noalta, fechaaltatrunc, nocontrato, noorden, importe, uuid
FROM eseotres.df_altas 
ORDER BY fechaaltatrunc DESC 
LIMIT 10;


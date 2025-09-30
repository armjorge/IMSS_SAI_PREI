 

    def update_sql_historico(self):
        print("🔄 Integrando información...")
        print("Fuente de las altas históricas")
        
        df_final = self.altas_historicas()
        print(df_final.head(2))
        try:
            df_final[['fechaAltaTrunc', 'fpp']] = df_final[['fechaAltaTrunc', 'fpp']].apply(pd.to_datetime, errors='coerce', format='%d/%m/%Y')
            df_final = self.sql_integration.sql_column_correction(df_final)         
            self.sql_integration.update_sql(df_final, esquema, tabla)
            # Cambio a diccionario
            print(f"✅ Actualización {esquema}.{tabla} completada")
        except Exception as e:
            print(f"❌ Error durante la actualización: {e}")
        
        print("✅ Integración completada")    
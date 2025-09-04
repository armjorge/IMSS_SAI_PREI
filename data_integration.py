import pandas as pd
import datetime 
import os
import glob
from helpers import message_print, create_directory_if_not_exists


class DataIntegration:
    def __init__(self, working_folder, data_access, integration_path):
        self.working_folder = working_folder
        self.data_access = data_access  
        self.integration_path = integration_path 
    def integrar_datos(self, prei_path, altas_path, facturas_path):
        print(f"🔍 Buscando archivos más recientes...")
        
        newest_altas_file, altas_date = self.get_newest_file(altas_path, "*.xlsx")
        newest_prei_file, prei_date = self.get_newest_file(prei_path, "*.xlsx")
        newest_facturas_file, facturas_date = self.get_newest_file(facturas_path, "*.xlsx")
        
        print(f"📊 Archivos encontrados:")
        print(f"   Altas: {newest_altas_file} ({altas_date})")
        print(f"   PREI: {newest_prei_file} ({prei_date})")
        print(f"   Facturas: {newest_facturas_file} ({facturas_date})")

        # Cargar DataFrames
        df_altas = pd.read_excel(newest_altas_file) if newest_altas_file else pd.DataFrame()
        df_prei = pd.read_excel(newest_prei_file) if newest_prei_file else pd.DataFrame()
        df_facturas = pd.read_excel(newest_facturas_file) if newest_facturas_file else pd.DataFrame()

        # Agregar fechas de archivo a cada DataFrame
        if not df_altas.empty and altas_date:
            df_altas['file_date'] = altas_date
        if not df_prei.empty and prei_date:
            df_prei['file_date'] = prei_date  
        if not df_facturas.empty and facturas_date:
            df_facturas['file_date'] = facturas_date

        if not df_altas.empty and not df_facturas.empty:  # ✅ Cambié 'and df_facturas.empty' por 'and not df_facturas.empty'
            left_columns = df_altas[['noAlta', 'noOrden']]
            right_columns = df_facturas[['Alta', 'Referencia']]
            UUID_column = 'UUID'
            return_column = df_facturas[UUID_column]
            
            df_altas[UUID_column] = self.validate_multiple_fields(left_columns, right_columns, return_column, unique=True)
            print(message_print(f"✅ Columna {UUID_column} agregada a df_altas"))

        if not df_altas.empty and not df_prei.empty:  # ✅ Cambié 'and df_facturas.empty' por 'and not df_facturas.empty'
            left_columns = df_altas[['UUID', 'importe']]
            right_columns = df_prei[['Folio Fiscal', 'Importe']]
            columna_cr = 'Estado C.R.'
            return_column_CR = df_prei[columna_cr]
            df_altas[columna_cr] = self.validate_multiple_fields(left_columns, right_columns, return_column_CR, unique=True)
            print(message_print(f"✅ Columna {columna_cr} agregada a df_altas"))

        # Crear carpeta de integración
        
        
        create_directory_if_not_exists(self.integration_path)

        # Encontrar la fecha más antigua
        fechas_disponibles = []
        if altas_date:
            fechas_disponibles.append(altas_date)
        if prei_date:
            fechas_disponibles.append(prei_date)
        if facturas_date:
            fechas_disponibles.append(facturas_date)

        if fechas_disponibles:
            oldest_date = min(fechas_disponibles)
            oldest_date_str = oldest_date.strftime("%Y-%m-%d-%H")
            
            # Crear nombre del archivo
            xlsx_path = os.path.join(self.integration_path, f"{oldest_date_str}_Integracion.xlsx")
            
            try:
                # Guardar múltiples hojas en un archivo Excel
                with pd.ExcelWriter(xlsx_path, engine='openpyxl') as writer:
                    if not df_altas.empty:
                        df_altas.to_excel(writer, sheet_name='df_altas', index=False)
                        print(f"✅ Hoja 'df_altas' guardada con {len(df_altas)} filas")
                    
                    if not df_prei.empty:
                        df_prei.to_excel(writer, sheet_name='df_prei', index=False)
                        print(f"✅ Hoja 'df_prei' guardada con {len(df_prei)} filas")
                    
                    if not df_facturas.empty:
                        df_facturas.to_excel(writer, sheet_name='df_facturas', index=False)
                        print(f"✅ Hoja 'df_facturas' guardada con {len(df_facturas)} filas")
                
                print(f"\n🎉 ¡Integración completada exitosamente!")
                print(f"📁 Archivo guardado en: {xlsx_path}")
                print(f"📅 Fecha de integración basada en: {oldest_date_str}")
                
            except Exception as e:
                print(f"❌ Error al guardar archivo de integración: {e}")
        else:
            print("⚠️ No se encontraron archivos válidos para integrar")

    def validate_multiple_fields(self, left_columns, right_columns, return_column, unique=True):
        """
        Valida múltiples campos entre DataFrames y retorna una Serie con los valores correspondientes.
        
        Args:
            left_columns: DataFrame con las columnas de referencia (ej: df_altas[['noAlta', 'noOrden', 'importe']])
            right_columns: DataFrame con las columnas objetivo (ej: df_facturas[['Alta', 'Referencia', 'Importe']])
            return_column: Serie con los valores a retornar (ej: df_facturas['UUID'])
            unique: Si True, espera solo 1 resultado por fila
        
        Returns:
            pd.Series: Serie con los valores encontrados o mensajes de error
        """
        results = []
        
        if unique:
            for index, row in left_columns.iterrows():
                # Crear máscara de comparación para todas las columnas
                mask = pd.Series([True] * len(right_columns))
                
                # Aplicar filtros por cada columna
                for left_col, right_col in zip(left_columns.columns, right_columns.columns):
                    mask = mask & (right_columns[right_col] == row[left_col])
                
                # Filtrar DataFrame con la máscara
                filtered_df = right_columns[mask]
                filtered_df = filtered_df.drop_duplicates()

                if filtered_df.shape[0] == 1:
                    # Solo un resultado encontrado
                    matching_index = filtered_df.index[0]
                    result_value = return_column.iloc[matching_index]
                    results.append(result_value)
                    
                elif filtered_df.shape[0] == 0:
                    # No se encontraron resultados
                    # Aquí puedo hace otra función para retornar strings más complejas como las de Con match para orden y alta, sin match para importe. 
                    # Algo que oriente más al usuario en caso de no tener factura ligada. 
                    results.append("No localizado")
                    
                else:
                    # Múltiples resultados encontrados
                    matching_indices = filtered_df.index.tolist()
                    matching_values = return_column.iloc[matching_indices].tolist()
                    error_msg = f"Error: múltiples resultados ({len(matching_values)}):\n {'\n'.join(map(str, matching_values))}"
                    results.append(error_msg)
                    print(f"⚠️ Fila {index}: {error_msg}")
                    
                    # Debug: mostrar qué valores causaron duplicados
                    debug_values = []
                    for col in left_columns.columns:
                        debug_values.append(f"{col}={row[col]}")
                    print(f"   Valores de búsqueda: {', '.join(debug_values)}")
        
        # Retornar como Serie con el mismo índice que left_columns
        return pd.Series(results, index=left_columns.index)


    def get_newest_file(self, path, pattern="*.xlsx"): 
        """
        Obtiene el archivo más reciente basado en la fecha en el nombre del archivo.
        Formatos soportados: 
        - YYYY-MM-DD-HH-nombre.xlsx (ej: 2025-08-25-13-PREI.xlsx)
        - YYYY-MM-DD-HH_nombre.xlsx (ej: 2025-08-25-13_PAQ_IMSS.xlsx)
        - YYYY-MM-DD-HH-nombre-extra.xlsx (ej: 2025-08-25-12-SAI Altas.xlsx)
        """
        today = datetime.date.today()
        
        if not os.path.exists(path):
            print(f"⚠️ La carpeta {path} no existe")
            return None, None
        
        # Obtener todos los archivos que coincidan con el patrón
        files = glob.glob(os.path.join(path, pattern))
        
        if not files:
            print(f"⚠️ No se encontraron archivos {pattern} en {os.path.basename(path)}")
            return None, None
        
        newest_file = None
        newest_date = None
        
        for file_path in files:
            filename = os.path.basename(file_path)
            
            try:
                # Dividir el nombre por guiones
                parts = filename.split('-')
                
                # Necesitamos al menos 4 partes: YYYY, MM, DD, HH
                if len(parts) >= 4:
                    year = parts[0]
                    month = parts[1] 
                    day = parts[2]
                    hour = parts[3]
                    
                    # Limpiar la hora si tiene underscore o caracteres extra
                    # Ej: "13_PAQ" -> "13", "12" -> "12"
                    if '_' in hour:
                        hour = hour.split('_')[0]
                    elif ' ' in hour:
                        hour = hour.split(' ')[0]
                    # Si tiene extensión o más texto, tomar solo los primeros dígitos
                    hour = ''.join(filter(str.isdigit, hour))
                    
                    # Validar que todos sean números
                    if (year.isdigit() and month.isdigit() and 
                        day.isdigit() and hour.isdigit()):
                        
                        year_int = int(year)
                        month_int = int(month)
                        day_int = int(day)
                        hour_int = int(hour)
                        
                        # Crear datetime
                        file_date = datetime.datetime(year_int, month_int, day_int, hour_int, 0)
                        
                        if newest_date is None or file_date > newest_date:
                            newest_date = file_date
                            newest_file = file_path
                        
                        print(f"🔍 {filename} → {file_date.strftime('%Y-%m-%d %H:%M')}")
                    else:
                        print(f"⚠️ Formato de fecha inválido en: {filename}")
                        
            except (ValueError, IndexError) as e:
                print(f"⚠️ No se pudo extraer fecha de {filename}: {e}")
                continue
        
        if newest_file:
            file_date_only = newest_date.date()
            
            # Verificar si el archivo es de hoy
            if file_date_only < today:
                print(f"⚠️ El archivo {os.path.basename(newest_file)} no es de hoy ({file_date_only}), se recomienda descargar")
            
            print(f"✅ Archivo más reciente: {os.path.basename(newest_file)} ({newest_date.strftime('%Y-%m-%d %H:%M')})")
            return newest_file, newest_date
        else:
            print(f"❌ No se pudo determinar el archivo más reciente en {os.path.basename(path)}")
            return None, None

    def run_queries(self, queries_folder, schema, table_name):
        """Ejecuta las consultas SQL en el folder especificado."""
        print(f"🔄 Ejecutando consultas en {queries_folder}...")
        for query_file in glob.glob(os.path.join(queries_folder, "*.sql")):
            with open(query_file, "r") as f:
                query = f.read()
                self.execute_query(query, schema, table_name)
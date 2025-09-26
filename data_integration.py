import pandas as pd
import datetime 
import os
import glob

from sympy import group
from helpers import message_print, create_directory_if_not_exists
import yaml
import re
from collections import defaultdict

class DataIntegration:
    def __init__(self, working_folder, data_access, integration_path):
        self.working_folder = working_folder
        self.data_access = data_access  
        self.integration_path = integration_path 
        
        self.prei_path=os.path.join(self.working_folder, "PREI", "PREI_files")
        self.altas_path=os.path.join(self.working_folder, "SAI", "SAI Altas_files")
        self.facturas_path=os.path.join(self.working_folder, "Facturas", "Consultas")
        self.ordenes_path=os.path.join(self.working_folder, "SAI", "SAI Orders_files")
        self.folders = {
            "prei": self.prei_path,
            "altas": self.altas_path,
            "facturas": self.facturas_path,
            "ordenes": self.ordenes_path
            }

    def generate_file_groups(self):
        print(self.folders)
        from datetime import datetime, timedelta
        print(f"🔍 Buscando archivos más recientes...")
        # Regex para extraer yyyy-mm-dd-hh
        ts_pattern = re.compile(r"^(\d{4}-\d{2}-\d{2}-\d{2})")
        
        # 1. Escanear todos los archivos
        all_files = []
        for cat, folder in self.folders.items():
            if not os.path.exists(folder):
                continue
            for f in os.listdir(folder):
                if f.endswith(".xlsx"):
                    m = ts_pattern.match(f)
                    if m:
                        ts = datetime.strptime(m.group(1), "%Y-%m-%d-%H")
                        all_files.append((ts, cat, os.path.join(folder, f)))

        # 2. Ordenar por timestamp
        all_files.sort(key=lambda x: x[0])

        # 3. Agrupar con ventana de 2 horas
        groups = []
        current = []
        for ts, cat, path in all_files:
            if not current:
                current.append((ts, cat, path))
            else:
                delta = ts - current[0][0]
                if delta <= timedelta(hours=2):
                    current.append((ts, cat, path))
                else:
                    groups.append(current)
                    current = [(ts, cat, path)]
        if current:
            groups.append(current)

        # 4. Formar all_groups y complete_groups
        all_groups = []
        complete_groups = []
        for g in groups:
            min_ts = min(x[0] for x in g)
            max_ts = max(x[0] for x in g)
            group_id = f"{min_ts.strftime('%Y-%m-%d-%H')}_{max_ts.strftime('%H')}"
            record = {"group_id": group_id}
            for cat in self.folders.keys():
                record[cat] = ""
            for ts, cat, path in g:
                record[cat] = path
            all_groups.append(record)

            if all(record[cat] for cat in self.folders.keys()):
                complete_groups.append(record)

        print("📂 all_groups encontrados:", len(all_groups))
        print("✅ complete_groups completos:", len(complete_groups))
       # 🔎 Analizar si los date-hour coinciden dentro de cada grupo
        exact_match_count = 0
        different_count = 0
        for g in all_groups:
            hours = []
            for cat in self.folders.keys():
                if g[cat]:
                    # Tomamos solo yyyy-mm-dd-hh del path
                    fname = os.path.basename(g[cat])
                    ts_prefix = fname[:13]  # YYYY-MM-DD-HH
                    hours.append(ts_prefix)
            if hours and all(h == hours[0] for h in hours):
                exact_match = True
                exact_match_count += 1
            else:
                exact_match = False
                different_count += 1
            print(f"Grupo {g['group_id']} → same_datehour == {exact_match}")

        print(f"📊 Grupos con misma fecha-hora exacta: {exact_match_count}")
        print(f"📊 Grupos con diferencias de hora: {different_count}")

        return complete_groups
             

    def integrar_datos(self):
        print(f"🔍 Buscando archivos más recientes...")
        group_preffix_file = self.generate_file_groups()
        for group in group_preffix_file:
            # Cargamos dataframes 
            df_altas    = pd.read_excel(group['altas'])    if group['altas']    else pd.DataFrame()
            df_prei     = pd.read_excel(group['prei'])     if group['prei']     else pd.DataFrame()
            df_facturas = pd.read_excel(group['facturas']) if group['facturas'] else pd.DataFrame()
            df_ordenes  = pd.read_excel(group['ordenes'])  if group['ordenes']  else pd.DataFrame()
            # Generamos fecha de grupo de archivos 
            prefix = group['group_id'].split("_")[0]   # "2025-09-19-08"
            dt = datetime.datetime.strptime(prefix, "%Y-%m-%d-%H")
            group_date = dt.replace(minute=0, second=0, microsecond=0)

            # Agregar fechas de archivo a cada DataFrame
            if not df_altas.empty:
                df_altas['file_date'] = group_date
            if not df_prei.empty:
                df_prei['file_date'] = group_date
            if not df_facturas.empty:
                df_facturas['file_date'] = group_date
            if not df_ordenes.empty:
                df_ordenes['file_date'] = group_date
            altas_invoice_join = {'left': ['noAlta', 'noOrden'], 'right': ['Alta', 'Referencia'], 'return': ['UUID']}
            df_altas = self.populate_df(df_altas, df_facturas, altas_invoice_join)
            altas_prei_join = {'left': ['UUID'], 'right': ['Folio Fiscal'], 'return': ['Estado C.R.']}
            df_altas = self.populate_df(df_altas, df_prei, altas_prei_join)
            output_file_name = f'{prefix}_Integracion.xlsx' 
            output_file_path = os.path.join(self.integration_path, output_file_name)
            try:
                # Guardar múltiples hojas en un archivo Excel
                with pd.ExcelWriter(output_file_path, engine='openpyxl') as writer:
                    if not df_altas.empty:
                        df_altas.to_excel(writer, sheet_name='df_altas', index=False)
                        print(f"✅ Hoja 'df_altas' guardada con {len(df_altas)} filas")
                    
                    if not df_prei.empty:
                        df_prei.to_excel(writer, sheet_name='df_prei', index=False)
                        print(f"✅ Hoja 'df_prei' guardada con {len(df_prei)} filas")
                    
                    if not df_facturas.empty:
                        df_facturas.to_excel(writer, sheet_name='df_facturas', index=False)
                        print(f"✅ Hoja 'df_facturas' guardada con {len(df_facturas)} filas")
                    if not df_ordenes.empty:
                        df_ordenes.to_excel(writer, sheet_name='df_ordenes', index=False)
                        print(f"✅ Hoja 'df_ordenes' guardada con {len(df_ordenes)} filas")
                                
                print(f"\n🎉 ¡Integración completada exitosamente!")
                print(f"📁 Archivo guardado en: {os.path.basename(output_file_path)}")

                
            except Exception as e:
                print(f"❌ Error al guardar archivo de integración: {e}")
        else:
            print("⚠️ No se encontraron archivos válidos para integrar")                    

    def populate_df(self, left_df, right_df, query_dict):
        """
        Pobla columnas en left_df a partir de right_df según query_dict.
        
        query_dict:
            {
                'left': ['col1_left', 'col2_left'],
                'right': ['col1_right', 'col2_right'],
                'return': ['colX_right', 'colY_right']
            }
        """
        left_keys = query_dict['left']
        right_keys = query_dict['right']
        return_cols = query_dict['return']

        # Validación
        if len(left_keys) != len(right_keys):
            raise ValueError("Las llaves left y right deben tener la misma longitud")
        # Validación de existencia de columnas en left_df
        missing_left = [col for col in left_keys if col not in left_df.columns]
        if missing_left:
            print(f"⚠️ Columnas faltantes en left_df: {', '.join(missing_left)}. No se puede proceder con el merge.")
            return left_df

        # Validación de existencia de columnas en right_df para keys
        missing_right_keys = [col for col in right_keys if col not in right_df.columns]
        if missing_right_keys:
            print(f"⚠️ Columnas faltantes en right_df para keys: {', '.join(missing_right_keys)}. No se puede proceder con el merge.")
            return left_df

        # Validación de existencia de columnas en right_df para return
        missing_return = [col for col in return_cols if col not in right_df.columns]
        if missing_return:
            print(f"⚠️ Columnas faltantes en right_df para return: {', '.join(missing_return)}. No se puede proceder con el merge.")
            return left_df

        # Índice compuesto para búsquedas rápidas
        right_index = right_df.groupby(right_keys)[return_cols].agg(lambda x: ','.join(x.astype(str))).reset_index()

        # Hacer merge left→right (left join)
        merged = pd.merge(
            left_df,
            right_index,
            how="left",
            left_on=left_keys,
            right_on=right_keys,
            suffixes=('', '_right')
        )

        # Rellenar NaN con "no localizado"
        for col in return_cols:
            if col in merged.columns:
                merged[col] = merged[col].fillna("no localizado")

        # Eliminar columnas auxiliares de join (las right_keys)
        merged = merged.drop(columns=right_keys, errors="ignore")

        return merged


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

if __name__ == "__main__":
    folder_root = os.getcwd()
    working_folder = os.path.join(folder_root, "Implementación")
    yaml_path = os.path.join(working_folder, "config.yaml")
    yaml_exists = os.path.exists(yaml_path)
    integration_path = os.path.join(working_folder, "Integración")
    if yaml_exists:
        # Abrir y cargar el contenido YAML en un diccionario
        with open(yaml_path, 'r', encoding='utf-8') as f:
            data_access = yaml.safe_load(f)
        print(f"✅ Archivo YAML cargado correctamente: {os.path.basename(yaml_path)}")
    app = DataIntegration(working_folder, data_access, integration_path)
    app.integrar_datos()
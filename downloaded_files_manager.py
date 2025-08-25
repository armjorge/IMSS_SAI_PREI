import os
import yaml
from helpers import message_print, create_directory_if_not_exists
import pandas as pd
import datetime
import platform

class DownloadedFilesManager:
    def __init__(self, working_folder, data_access):
        self.working_folder = working_folder
        self.data_access = data_access

    def manage_downloaded_files(self, path_input):
        # Hacemos absoluto el path de salida
        sub_path = os.path.abspath(os.path.join(path_input, ".."))
        preffix = os.path.basename(sub_path)
        
        #Lógica para extraer la función
        #files_to_process = 
        print(f"Procesando archivos desde\n{os.path.basename(path_input)}\n")
        
        xlsx_list = [f for f in os.listdir(path_input) if f.endswith(".xlsx")]
        print(f"Archivos encontrados: {xlsx_list}")
        columns_altas = self.data_access['columns_IMSS_altas']
        altas_path = os.path.join(sub_path, f"{preffix} Altas_files")
        columns_orders = self.data_access['columns_IMSS_orders']
        orders_path = os.path.join(sub_path, f"{preffix} Orders_files")
        prei_output_path = os.path.join(sub_path, f"{preffix} PREI_files")


        altas_files = []
        altas_dates = []
        orders_files = []
        orders_dates = []        
        if len(xlsx_list) > 0:
            for file in xlsx_list:
                file_path = os.path.join(path_input, file)  # Este es el path completo del archivo
                df_file = pd.read_excel(file_path)  # ✅ Usar file_path directamente
                file_creation_date = self.get_file_creation_date(file_path)  # ✅ Pasar file_path completo
                formatted_date = self.format_date_for_filename(file_creation_date)
                if df_file.columns.tolist() == columns_altas:
                    print(f"Archivo {file} identificado como Altas (creado: {file_creation_date})")
                    altas_files.append(file_path)
                    altas_dates.append(formatted_date)
                elif df_file.columns.tolist() == columns_orders:
                    print(f"Archivo {file} identificado como Orders")
                    orders_files.append(file_path)
                    orders_dates.append(formatted_date)
        else:
            print("❌ No se encontraron archivos .xlsx en la carpeta de descargas temporales.")
        
        xls_list = [f for f in os.listdir(path_input) if f.endswith(".xls")]
        prei_files = []
        prei_dates = []
        if len(xls_list) > 0:
            for file in xls_list:
                file_path = os.path.join(path_input, file)
                df_file = self.XLS_header_location(file_path)
                if df_file is not None: 
                    prei_files.append(file_path)
                    file_creation_date = self.get_file_creation_date(file_path)  # ✅ Pasar file_path completo
                    formatted_date = self.format_date_for_filename(file_creation_date)
                    prei_dates.append(formatted_date)
        else:
            print("❌ No se encontraron archivos .xls en la carpeta de descargas temporales.")
        print(prei_files)
        print(prei_dates)
        

    def get_file_creation_date(self, file_path):
        """
        Extrae la fecha de creación del archivo de manera precisa en Windows y Mac
        Returns: datetime object with the creation date
        """
        try:
            file_stats = os.stat(file_path)
            
            # En Windows, st_ctime es la fecha de creación
            # En Unix/Mac, st_ctime es la fecha de cambio de metadatos
            # En Mac, usamos st_birthtime si está disponible
            if platform.system() == 'Windows':
                creation_timestamp = file_stats.st_ctime
            elif platform.system() == 'Darwin':  # macOS
                # En macOS, st_birthtime es más preciso para fecha de creación
                creation_timestamp = getattr(file_stats, 'st_birthtime', file_stats.st_ctime)
            else:  # Linux y otros Unix
                # En Linux no hay fecha de creación real, usamos st_ctime
                creation_timestamp = file_stats.st_ctime
            
            return datetime.datetime.fromtimestamp(creation_timestamp)
        
        except Exception as e:
            print(f"Error al obtener fecha de creación de {file_path}: {e}")
            # Fallback: usar fecha de modificación
            return datetime.datetime.fromtimestamp(os.path.getmtime(file_path))

    def format_date_for_filename(self, date_obj):
        """
        Formatea la fecha para usar en nombres de archivo
        Returns: string en formato YYYY-MM-DD_HHMMSS
        """
        return date_obj.strftime("%Y-%m-%d_%H%M%S")
    def XLS_header_location(self, filepath):
        """
        Busca en las primeras 10 filas del archivo XLS para encontrar los headers correctos
        que coincidan con columns_PREI y retorna el DataFrame con los headers correctos.
        """
        columns_PREI = self.data_access['columns_PREI']
        
        # Leer las primeras 11 filas (0-10) sin headers
        df_raw = pd.read_excel(filepath, header=None, nrows=11)
        
        header_row = None
        
        # Buscar en cada fila (0-10) los headers que coincidan
        for row_index in range(min(11, len(df_raw))):
            # Obtener los valores de la fila como lista
            potential_headers = df_raw.iloc[row_index].tolist()
            
            # Limpiar valores None, NaN y convertir a string
            potential_headers = [str(col).strip() if pd.notna(col) else '' for col in potential_headers]
            
            # Filtrar solo valores no vacíos
            potential_headers = [col for col in potential_headers if col != '' and col != 'nan']
            
            print(f"🔍 Fila {row_index}: {potential_headers}")
            
            # Verificar si coincide con columns_PREI
            if potential_headers == columns_PREI:
                header_row = row_index
                print(f"✅ Headers encontrados en fila {row_index}")
                break
        
        if header_row is not None:
            # Leer el archivo completo usando la fila correcta como header
            df_final = pd.read_excel(filepath, header=header_row)
            print(f"📊 DataFrame creado con {len(df_final)} filas y columnas: {df_final.columns.tolist()}")
            return df_final
        else:
            print(f"❌ No se encontraron headers que coincidan con columns_PREI: {columns_PREI}")
            print(f"📋 Headers esperados: {columns_PREI}")
            # Retornar DataFrame vacío o usar la primera fila como fallback
            return None

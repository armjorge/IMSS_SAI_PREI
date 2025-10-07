import os
import yaml
import pandas as pd
import datetime
import platform
import hashlib


class DownloadedFilesManager:
    def __init__(self, working_folder, data_access):
        self.working_folder = working_folder
        self.data_access = data_access

    def manage_downloaded_files(self, path_input):
        # Path de salida basado en la carpeta padre
        sub_path = os.path.abspath(os.path.join(path_input, ".."))
        preffix = os.path.basename(sub_path)

        print(f"Procesando archivos desde\n{os.path.basename(path_input)}\n")

        # Clasificación de archivos .xlsx (ALTAS y ORDERS)
        xlsx_list = [f for f in os.listdir(path_input) if f.lower().endswith(".xlsx")]
        print(f"Archivos .xlsx encontrados: {xlsx_list}")
        columns_altas = self.data_access['columns_IMSS_altas']
        altas_path = os.path.join(sub_path, f"{preffix} Altas_files")
        columns_orders = self.data_access['columns_IMSS_orders']
        orders_path = os.path.join(sub_path, f"{preffix} Orders_files")
        prei_output_path = os.path.join(sub_path, f"{preffix}_files")

        altas_files = []
        altas_dates = []
        orders_files = []
        orders_dates = []
        prei_files = []
        prei_dates = []

        if xlsx_list:
            for file in xlsx_list:
                file_path = os.path.join(path_input, file)
                try:
                    df_file = pd.read_excel(file_path)
                except Exception as e:
                    print(f"No se pudo leer {file}: {e}")
                    continue

                file_creation_date = self.get_file_creation_date(file_path)
                formatted_date = self.format_date_for_filename(file_creation_date)

                cols = df_file.columns.tolist()
                # Normalizar para comparar de manera robusta
                cols_norm = self._normalize_cols(cols)
                altas_norm = self._normalize_cols(columns_altas)
                orders_norm = self._normalize_cols(columns_orders)

                match_altas = (cols_norm == altas_norm) or (set(altas_norm).issubset(set(cols_norm)))
                match_orders = (cols_norm == orders_norm) or (set(orders_norm).issubset(set(cols_norm)))

                if match_altas:
                    print(f"Archivo {file} identificado como Altas (creado: {file_creation_date})")
                    altas_files.append(file_path)
                    altas_dates.append(formatted_date)
                elif match_orders:
                    print(f"Archivo {file} identificado como Orders (creado: {file_creation_date})")
                    orders_files.append(file_path)
                    orders_dates.append(formatted_date)
                else:
                    # Fallback por nombre de archivo
                    fname = os.path.basename(file_path).lower()
                    if 'alta' in fname:
                        print(f"Fallback por nombre: {file} clasificado como Altas")
                        altas_files.append(file_path)
                        altas_dates.append(formatted_date)
                    elif 'orden' in fname or 'order' in fname:
                        print(f"Fallback por nombre: {file} clasificado como Orders")
                        orders_files.append(file_path)
                        orders_dates.append(formatted_date)
                    else:
                        print(f"Advertencia: {file} no coincide por headers. Cols: {cols}")
        else:
            print("No se encontraron archivos .xlsx en la carpeta de descargas temporales.")

        # Clasificación de archivos .xls (PREI)
        xls_list = [f for f in os.listdir(path_input) if f.lower().endswith(".xls")]
        if xls_list:
            for file in xls_list:
                file_path = os.path.join(path_input, file)
                df_file = self.XLS_header_location(file_path)
                if df_file is not None:
                    prei_files.append(file_path)
                    file_creation_date = self.get_file_creation_date(file_path)
                    formatted_date = self.format_date_for_filename(file_creation_date)
                    prei_dates.append(formatted_date)
        else:
            print("No se encontraron archivos .xls en la carpeta de descargas temporales.")

        # ALTAS: combinar (evitando duplicados de archivo) y mover con nombre unificado
        if altas_files:
            os.makedirs(altas_path, exist_ok=True)

            # Evitar archivos duplicados (mismo contenido) por hash
            unique_altas_files = []
            seen_hashes = set()
            for f in altas_files:
                try:
                    h = self._file_sha256(f)
                except Exception as e:
                    print(f"Advertencia: no se pudo calcular hash de {os.path.basename(f)}: {e}. Se incluirá igualmente.")
                    h = None
                if (h is None) or (h not in seen_hashes):
                    unique_altas_files.append(f)
                    if h is not None:
                        seen_hashes.add(h)

            if unique_altas_files:
                df_altas = pd.DataFrame()
                kept_dates = []
                for f in unique_altas_files:
                    try:
                        df = pd.read_excel(f)
                        df_altas = pd.concat([df_altas, df], ignore_index=True)
                        # mapear fecha correspondiente al archivo original
                        try:
                            idx = altas_files.index(f)
                            kept_dates.append(altas_dates[idx])
                        except Exception:
                            pass
                    except Exception as e:
                        print(f"Error leyendo {os.path.basename(f)}: {e}")

                if not df_altas.empty:
                    unique_dates = sorted(list(set(kept_dates))) if kept_dates else []
                    if len(unique_dates) == 1:
                        date_str = unique_dates[0]
                    elif len(unique_dates) > 1:
                        date_str = f"{unique_dates[0]}_to_{unique_dates[-1]}"
                    else:
                        date_str = self.format_date_for_filename(datetime.datetime.now())

                    filename = f"{date_str}-{preffix} Altas.xlsx"
                    output_path = os.path.join(altas_path, filename)
                    try:
                        df_altas.to_excel(output_path, index=False)
                        print(f"Archivo ALTAS combinado guardado: {filename}")
                        print(f"Total de filas ALTAS: {len(df_altas)}")
                        # eliminar originales
                        for f in altas_files:
                            try:
                                os.remove(f)
                                print(f"Eliminado original ALTAS: {os.path.basename(f)}")
                            except Exception as e:
                                print(f"Error eliminando {os.path.basename(f)}: {e}")
                    except Exception as e:
                        print(f"Error guardando archivo ALTAS combinado: {e}")
            else:
                print("No hay archivos ALTAS únicos para combinar.")

        # ORDERS: combinar (evitando duplicados de archivo) y mover con nombre unificado
        if orders_files:
            os.makedirs(orders_path, exist_ok=True)

            # Evitar archivos duplicados (mismo contenido) por hash
            unique_orders_files = []
            seen_hashes = set()
            for f in orders_files:
                try:
                    h = self._file_sha256(f)
                except Exception as e:
                    print(f"Advertencia: no se pudo calcular hash de {os.path.basename(f)}: {e}. Se incluirá igualmente.")
                    h = None
                if (h is None) or (h not in seen_hashes):
                    unique_orders_files.append(f)
                    if h is not None:
                        seen_hashes.add(h)

            if unique_orders_files:
                df_orders = pd.DataFrame()
                kept_dates = []
                for f in unique_orders_files:
                    try:
                        df = pd.read_excel(f)
                        df_orders = pd.concat([df_orders, df], ignore_index=True)
                        # mapear fecha correspondiente al archivo original
                        try:
                            idx = orders_files.index(f)
                            kept_dates.append(orders_dates[idx])
                        except Exception:
                            pass
                    except Exception as e:
                        print(f"Error leyendo {os.path.basename(f)}: {e}")

                if not df_orders.empty:
                    unique_dates = sorted(list(set(kept_dates))) if kept_dates else []
                    if len(unique_dates) == 1:
                        date_str = unique_dates[0]
                    elif len(unique_dates) > 1:
                        date_str = f"{unique_dates[0]}_to_{unique_dates[-1]}"
                    else:
                        date_str = self.format_date_for_filename(datetime.datetime.now())

                    filename = f"{date_str}-{preffix} Orders.xlsx"
                    output_path = os.path.join(orders_path, filename)
                    try:
                        df_orders.to_excel(output_path, index=False)
                        print(f"Archivo ORDERS combinado guardado: {filename}")
                        print(f"Total de filas ORDERS: {len(df_orders)}")
                        # eliminar originales
                        for f in orders_files:
                            try:
                                os.remove(f)
                                print(f"Eliminado original ORDERS: {os.path.basename(f)}")
                            except Exception as e:
                                print(f"Error eliminando {os.path.basename(f)}: {e}")
                    except Exception as e:
                        print(f"Error guardando archivo ORDERS combinado: {e}")
            else:
                print("No hay archivos ORDERS únicos para combinar.")

        # PREI: ya existía lógica de combinación; la mantenemos
        if prei_files:
            os.makedirs(prei_output_path, exist_ok=True)
            df_prei = pd.DataFrame()

            for file_path in prei_files:
                df_file = self.XLS_header_location(file_path)
                if df_file is not None and not df_file.empty:
                    df_prei = pd.concat([df_prei, df_file], ignore_index=True)
                    print(f"Archivo {os.path.basename(file_path)} agregado al DataFrame PREI combinado")
                else:
                    print(f"Archivo {os.path.basename(file_path)} omitido (vacío o sin headers válidos)")

            if not df_prei.empty:
                unique_dates = sorted(list(set(prei_dates))) if prei_dates else []
                if len(unique_dates) == 1:
                    date_str = unique_dates[0]
                elif len(unique_dates) > 1:
                    date_str = f"{unique_dates[0]}_to_{unique_dates[-1]}"
                else:
                    date_str = self.format_date_for_filename(datetime.datetime.now())

                filename = f"{date_str}-{preffix}.xlsx"
                output_file_path = os.path.join(prei_output_path, filename)

                try:
                    df_prei.to_excel(output_file_path, index=False)
                    print(f"Archivo PREI combinado guardado: {filename}")
                    print(f"Total de filas PREI: {len(df_prei)}")

                    # Eliminar archivos originales después de combinar
                    for file_path in prei_files:
                        try:
                            os.remove(file_path)
                            print(f"Archivo PREI original eliminado: {os.path.basename(file_path)}")
                        except Exception as e:
                            print(f"Error eliminando {os.path.basename(file_path)}: {e}")
                except Exception as e:
                    print(f"Error guardando archivo PREI combinado: {e}")
            else:
                print("No se encontraron datos válidos en los archivos PREI")

    def _file_sha256(self, file_path, chunk_size=65536):
        sha256 = hashlib.sha256()
        with open(file_path, 'rb') as f:
            for chunk in iter(lambda: f.read(chunk_size), b''):
                sha256.update(chunk)
        return sha256.hexdigest()

    def _normalize_cols(self, cols):
        def norm_one(x):
            try:
                s = str(x)
            except Exception:
                s = ''
            # Normalizaciones básicas: trim, lower, colapsar espacios
            s = s.replace('\u00a0', ' ')  # NBSP a espacio normal
            s = ' '.join(s.strip().split())
            return s.lower()
        return [norm_one(c) for c in cols]

    def get_file_creation_date(self, file_path):
        """
        Extrae la fecha de creación del archivo de manera precisa en Windows y Mac
        Returns: datetime object with the creation date
        """
        try:
            file_stats = os.stat(file_path)

            if platform.system() == 'Windows':
                creation_timestamp = file_stats.st_ctime
            elif platform.system() == 'Darwin':  # macOS
                creation_timestamp = getattr(file_stats, 'st_birthtime', file_stats.st_ctime)
            else:  # Linux y otros Unix
                creation_timestamp = file_stats.st_ctime

            return datetime.datetime.fromtimestamp(creation_timestamp)

        except Exception as e:
            print(f"Error al obtener fecha de creación de {file_path}: {e}")
            # Fallback: usar fecha de modificación
            return datetime.datetime.fromtimestamp(os.path.getmtime(file_path))

    def format_date_for_filename(self, date_obj):
        """
        Formatea la fecha para usar en nombres de archivo
        Returns: string en formato YYYY-MM-DD-HH
        """
        return date_obj.strftime("%Y-%m-%d-%H")

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
            potential_headers = df_raw.iloc[row_index].tolist()
            # Limpiar valores None, NaN y convertir a string
            potential_headers = [str(col).strip() if pd.notna(col) else '' for col in potential_headers]
            # Filtrar solo valores no vacíos
            potential_headers = [col for col in potential_headers if col != '' and col != 'nan']

            print(f"Fila {row_index}: {potential_headers}")

            # Verificar si coincide con columns_PREI
            if potential_headers == columns_PREI:
                header_row = row_index
                print(f"Headers PREI encontrados en fila {row_index}")
                break

        if header_row is not None:
            # Leer el archivo completo usando la fila correcta como header
            df_final = pd.read_excel(filepath, header=header_row)
            print(f"DataFrame PREI creado con {len(df_final)} filas y columnas: {df_final.columns.tolist()}")
            return df_final
        else:
            print(f"No se encontraron headers que coincidan con columns_PREI: {columns_PREI}")
            print(f"Headers esperados: {columns_PREI}")
            return None

import os
import pickle
import subprocess
import pandas as pd


class Helper:
    @staticmethod
    def message_print(message):
        """Formatea mensajes con asteriscos para destacarlos"""
        message_highlights = '*' * len(message)
        return f'\n{message_highlights}\n{message}\n{message_highlights}\n'

    @staticmethod
    def create_directory_if_not_exists(path_or_paths):
        """Crea directorios si no existen"""
        if isinstance(path_or_paths, str):
            paths = [path_or_paths]
        else:
            paths = path_or_paths

        for path in paths:
            if not os.path.exists(path):
                print(f"\tCreando directorio: {os.path.basename(path)}")
                os.makedirs(path)
            else:
                print(f"\tDirectorio encontrado: {os.path.basename(path)}")

    @staticmethod
    def add_to_gitignore(root_directory, path_to_add):
        """Añade una ruta al archivo .gitignore"""
        gitignore_path = os.path.join(root_directory, ".gitignore")
        relative_output = f"{os.path.basename(path_to_add)}/"

        if os.path.exists(gitignore_path):
            with open(gitignore_path, 'r') as f:
                lines = f.read().splitlines()
        else:
            lines = []

        if relative_output not in lines:
            with open(gitignore_path, 'a') as f:
                f.write(f"\n{relative_output}\n")
            print(f"'{relative_output}' agregado a .gitignore.")

    @staticmethod
    def open_folder(os_path):
        """Abre una carpeta en el explorador de archivos"""
        try:
            if os.name == 'nt':  # Windows
                os.startfile(os_path)
            elif os.name == 'posix':  # macOS o Linux
                if "darwin" in os.uname().sysname.lower():  # macOS
                    subprocess.run(["open", os_path])
                else:  # Linux
                    subprocess.run(["xdg-open", os_path])
        except Exception as e:
            print(f"Error opening folder: {e}")

    @staticmethod
    def load_pickle_as_dataframe(file_path):
        """Carga un archivo pickle como DataFrame si existe y es válido"""
        if not os.path.exists(file_path):
            print(f"File not found: {file_path}")
            return None
        try:
            with open(file_path, 'rb') as f:
                df = pickle.load(f)
                return df
        except Exception as e:
            print(f"Error loading pickle file: {e}")
            return None
    @staticmethod
    def save_dataframe_to_pickle(dataframe, pickle_path):
        """
        Compara un DataFrame nuevo contra el almacenado en un pickle (si existe),
        muestra un resumen de diferencias y pide confirmación para sobrescribir.

        Params
        - dataframe: pandas.DataFrame a guardar
        - pickle_path: ruta del archivo pickle destino
        """
        import pandas as pd
        import numpy as np
        try:
            if not isinstance(dataframe, pd.DataFrame):
                print("❌ El argumento 'dataframe' debe ser un pandas.DataFrame")
                return

            # Cargar el DataFrame actual si existe
            df_actual = Helper.load_pickle_as_dataframe(pickle_path)
            
            # Comparar y mostrar heads solo si hay diferencias
            if df_actual is not None and isinstance(df_actual, pd.DataFrame):
                if df_actual.equals(dataframe):
                    print("Los DataFrames son iguales.")
                    print("Head de ambos:\n", df_actual.head())
                else:
                    # Encontrar filas diferentes (usando columnas comunes)
                    common_cols = list(set(df_actual.columns) & set(dataframe.columns))
                    if common_cols:
                        df_actual_common = df_actual[common_cols].reset_index(drop=True)
                        dataframe_common = dataframe[common_cols].reset_index(drop=True)
                        diff_rows = pd.concat([df_actual_common, dataframe_common]).drop_duplicates(keep=False)
                        if not diff_rows.empty:
                            print("Filas diferentes (head):")
                            print(diff_rows.head())
                        else:
                            print("Los DataFrames son iguales (después de comparar columnas comunes).")
                            print("Head:\n", df_actual.head())
                    else:
                        print("No hay columnas en común para comparar.")
                        print("DF en el archivo \n", df_actual.head())
                        print("DF Actualizado \n", dataframe.head())
            else:
                print("No hay DF actual para comparar.")
                print("DF Actualizado \n", dataframe.head())
            # Resumen básico
            print("\n==== Resumen de diferencias del pickle ====")
            print(f"Destino: {pickle_path}")
            print(f"Nuevo: filas={len(dataframe)}, columnas={len(dataframe.columns)}")
            if df_actual is None:
                print("Actual: no existe pickle actual")
            elif not isinstance(df_actual, pd.DataFrame):
                print("Actual: el contenido del pickle no es un DataFrame válido")
            else:
                print(f"Actual: filas={len(df_actual)}, columnas={len(df_actual.columns)}")

                # Diferencias de columnas
                cols_nuevo = set(map(str, dataframe.columns))
                cols_actual = set(map(str, df_actual.columns))
                cols_agregadas = sorted(list(cols_nuevo - cols_actual))
                cols_eliminadas = sorted(list(cols_actual - cols_nuevo))
                if cols_agregadas:
                    print(f"Columnas agregadas: {cols_agregadas}")
                if cols_eliminadas:
                    print(f"Columnas eliminadas: {cols_eliminadas}")

                # Diferencias de filas usando columnas en común
                columnas_comunes = [c for c in dataframe.columns if c in df_actual.columns]
                if columnas_comunes:
                    try:
                        comunes_ordenadas = list(map(str, columnas_comunes))
                        # Convertimos a string para comparación robusta y crear llaves por fila
                        claves_nuevo = set(
                            map(
                                tuple,
                                dataframe[comunes_ordenadas].astype(str).itertuples(index=False, name=None)
                            )
                        )
                        claves_actual = set(
                            map(
                                tuple,
                                df_actual[comunes_ordenadas].astype(str).itertuples(index=False, name=None)
                            )
                        )
                        filas_agregadas = len(claves_nuevo - claves_actual)
                        filas_eliminadas = len(claves_actual - claves_nuevo)
                        print(f"Filas agregadas (vs. actual): {filas_agregadas}")
                        print(f"Filas eliminadas (vs. actual): {filas_eliminadas}")
                    except Exception as e:
                        print(f"⚠️ No fue posible calcular diferencias de filas: {e}")
                else:
                    print("⚠️ No hay columnas en común para comparar filas.")

            # Confirmación del usuario
            respuesta = input("\n¿Deseas actualizar el pickle con el DataFrame nuevo? (s/N): ").strip().lower()
            if respuesta not in ("s", "si", "sí", "y", "yes"):
                print("Operación cancelada. No se modificó el pickle.")
                return

            # Asegurar carpeta destino
            carpeta = os.path.dirname(pickle_path)
            if carpeta:
                Helper.create_directory_if_not_exists(carpeta)

            # Guardar pickle
            with open(pickle_path, 'wb') as f:
                pickle.dump(dataframe, f)
            print(f"✅ Pickle actualizado correctamente: {pickle_path}")

        except Exception as e:
            print(f"❌ Error en save_dataframe_to_pickle: {e}")
    @staticmethod
    def get_files_in_directory(directory):
        """
        Busca archivos en un directorio dado.
        """
        if not isinstance(directory, (str, bytes, os.PathLike)):
            raise TypeError(f"El argumento 'directory' debe ser un string, bytes, o PathLike, pero se recibió: {type(directory)}")
        
        print(f"Buscando archivos en el directorio {directory}")
        return [f for f in os.listdir(directory) if os.path.isfile(os.path.join(directory, f))]
    @staticmethod
    def get_file_headers(file_path):
        import pandas as pd
        try:
            df = pd.read_csv(file_path, nrows=1)
            return df.columns.tolist()
        except Exception as e:
            print(f"Error al leer el archivo {file_path}: {e}")
            return []
    @staticmethod
    def merge_files(file_paths):
        """
        Fusiona múltiples archivos CSV en uno solo o devuelve el archivo si solo hay uno.
        Lanza un error si no se proporciona ningún archivo.
        """
        import pandas as pd
        import os

        try:
            # Validar que se proporcionen archivos
            if not file_paths or len(file_paths) == 0:
                raise ValueError("No se proporcionaron archivos para fusionar.")

            # Si solo hay un archivo, devolverlo directamente
            if len(file_paths) == 1:
                print(f"⚠️ Solo se proporcionó un archivo. No se realizará la fusión: {file_paths[0]}")
                return file_paths[0]

            # Fusionar múltiples archivos
            dfs = [pd.read_csv(file) for file in file_paths]
            merged_df = pd.concat(dfs, ignore_index=True)
            merged_file_path = os.path.join(os.path.dirname(file_paths[0]), "merged_file.csv")
            merged_df.to_csv(merged_file_path, index=False)
            print(f"✅ Archivos fusionados correctamente en: {merged_file_path}")
            return merged_file_path

        except Exception as e:
            print(f"❌ Error al fusionar archivos: {e}")
            return None
    @staticmethod
    def move_file(source, destination):
        """
        Mueve un archivo de una ubicación a otra.
        """
        import shutil
        try:
            shutil.move(source, destination)
            print(f"✅ Archivo movido de {source} a {destination}")
        except Exception as e:
            print(f"❌ Error al mover el archivo {source}: {e}")
    @staticmethod
    def archivo_corriente_reciente(fecha, suffix, type):
        """
        Genera un path dinámico válido basado en la fecha y el sufijo.
        """
        if type == 'cerrado': 
            path_dinamico = os.path.join(f"{fecha}{suffix}")
        elif type == 'corriente':
            formatted_today = fecha.strftime('%Y-%m-%d')
            periodo = fecha.strftime('%Y-%m')
            path_dinamico = os.path.join(f"{periodo}",f"{formatted_today}{suffix}")
        return path_dinamico
    @staticmethod
    def update_pickle(path_debito, pickle_debito):
        """
        Updates or creates a pickle file with the data from path_debito.
        Replaces the entire content of the pickle file with the new data.
        Adds 'file_date' and 'file_name' columns to the DataFrame.
        """
        import pandas as pd
        import os
        import pickle
        from datetime import datetime, timedelta
    
        def get_previous_day_path(path):
            """
            Generates the file path for the previous day, considering folder changes.
            """
            try:
                # Extract the date from the current file path
                file_date_str = os.path.basename(path).split('_')[0]
                file_date = datetime.strptime(file_date_str, '%Y-%m-%d')
    
                # Calculate the previous day
                previous_day = file_date - timedelta(days=1)
    
                # Generate the new folder and file path
                previous_folder = previous_day.strftime('%Y-%m')
                previous_file = previous_day.strftime('%Y-%m-%d') + '_debito.csv'
                previous_path = os.path.join(os.path.dirname(os.path.dirname(path)), previous_folder, previous_file)
    
                return previous_path
            except Exception as e:
                print(f"❌ Error al calcular el archivo del día anterior: {e}")
                return None
    
        try:
            # Check if path_debito exists
            while not os.path.exists(path_debito):
                separator = '!' * 50
                print(f"\n{separator}\n❌ Archivo del día de hoy no encontrado: {os.path.basename(path_debito)}\n{separator}\n")
                
                previous_path = get_previous_day_path(path_debito)
                if previous_path and os.path.exists(previous_path):
                    print(f"🔄 Intentando con el archivo del día anterior: {previous_path}")
                    path_debito = previous_path
                else:
                    print(f"❌ No se encontró archivo para el día anterior: {previous_path}")
                    return
    
            # Load the CSV file into a DataFrame
            df_debito = pd.read_csv(path_debito)
            print(f"✅ Archivo origen cargado: {path_debito}")
    
            # Add 'file_date' and 'file_name' columns
            file_date = pd.to_datetime(os.path.basename(path_debito).split('_')[0], errors='coerce')
            file_name = os.path.basename(path_debito)
            df_debito['file_date'] = file_date
            df_debito['file_name'] = file_name
    
            # Replace the content of the pickle file
            print(f"⚠️ Reemplazando el contenido del pickle: {pickle_debito}")
            with open(pickle_debito, 'wb') as f:
                pickle.dump(df_debito, f)
                print(f"✅ Pickle actualizado con nueva información: {pickle_debito}")
    
        except Exception as e:
            print(f"❌ Error al actualizar el pickle: {e}")

    @staticmethod
    def feed_new_pickles(pickle_folder, pickle_target, columns):
        """
        Feeds new CSV files into the pickle file if their records do not already exist.
        """
        import pandas as pd
        import os
        import pickle
        from datetime import datetime

        def get_file_creation_date(file_path):
            """
            Obtiene la fecha de creación del archivo desde las propiedades del sistema.
            """
            try:
                creation_time = os.path.getctime(file_path)
                return datetime.fromtimestamp(creation_time)
            except Exception as e:
                print(f"❌ Error al obtener la fecha de creación del archivo {file_path}: {e}")
                return None

        try:
            # Get all CSV files in the folder
            csv_files = [os.path.join(pickle_folder, f) for f in os.listdir(pickle_folder) if f.endswith('.csv')]
            #print(f"📂 Archivos CSV encontrados: {csv_files}")

            # Filter CSV files that match the required columns
            csv_grupo = []
            for file in csv_files:
                try:
                    df = pd.read_csv(file, nrows=1)
                    if set(columns).issubset(df.columns):
                        csv_grupo.append(file)
                except Exception as e:
                    print(f"❌ Error al leer columnas del archivo {file}: {e}")
            print(f"📋 Archivos CSV que cumplen con las columnas requeridas: {csv_grupo}")

            # Load the target pickle file
            if os.path.exists(pickle_target):
                with open(pickle_target, 'rb') as f:
                    df_target = pickle.load(f)
                print(f"✅ Pickle cargado: {pickle_target}")
            else:
                print(f"⚠️ Pickle no encontrado. Creando uno nuevo: {pickle_target}")
                df_target = pd.DataFrame(columns=columns + ['file_name', 'file_date'])

            # Iterate over filtered CSV files
            for file in csv_grupo:
                try:
                    # Load the CSV file
                    df = pd.read_csv(file)
                    print(f"📄 Procesando archivo: {file}")

                    # Exclude 'file_name' and 'file_date' from the comparison
                    df_check = df[columns]
                    df_target_check = df_target[columns]

                    # Check if records already exist in df_target
                    exists = df_check.apply(tuple, axis=1).isin(df_target_check.apply(tuple, axis=1))

                    if exists.all():
                        print(f"⚠️ Todos los registros del archivo {file} ya existen en el pickle.")
                        continue

                    # Add 'file_name' and 'file_date' columns
                    df['file_name'] = os.path.basename(file)
                    file_date = get_file_creation_date(file)
                    if file_date:
                        df['file_date'] = file_date

                    # Append new records to df_target
                    df_new = df[~exists]
                    df_target = pd.concat([df_target, df_new], ignore_index=True)

                    # Write back to pickle
                    with open(pickle_target, 'wb') as f:
                        pickle.dump(df_target, f)
                    print(f"✅ Pickle actualizado con nuevos registros del archivo: {file}")

                except Exception as e:
                    print(f"❌ Error al procesar el archivo {file}: {e}")

        except Exception as e:
            print(f"❌ Error en feed_new_pickles: {e}")
    @staticmethod
    def corrige_fechas(df, columna_fecha):
        """
        Convierte fechas en formato 'dd/mm/yyyy' (como texto) a 'yyyy-mm-dd' en la columna especificada.
        Si la fecha ya está en otro formato válido, la trunca al día.
        Imprime cuántos renglones fueron cambiados.
        """
        import pandas as pd

        if columna_fecha not in df.columns:
            print(f"⚠️ La columna '{columna_fecha}' no existe en el DataFrame.")
            return df

        cambios = 0
        df[columna_fecha] = df[columna_fecha].astype(str)

        for idx, valor in df[columna_fecha].items():
            nuevo_valor = valor
            if '/' in valor:
                try:
                    nuevo_valor = pd.to_datetime(valor, format='%d/%m/%Y', errors='raise')
                    # Truncar la hora a 00:00:00
                    nuevo_valor = nuevo_valor.replace(hour=0, minute=0, second=0, microsecond=0)
                except Exception:
                    nuevo_valor = valor
            else:
                try:
                    nuevo_valor = pd.to_datetime(valor, errors='raise')
                    nuevo_valor = nuevo_valor.replace(hour=0, minute=0, second=0, microsecond=0)
                except Exception:
                    nuevo_valor = valor

            if not pd.isnull(nuevo_valor) and str(nuevo_valor) != valor:
                df.at[idx, columna_fecha] = nuevo_valor
                cambios += 1

        # Convertir toda la columna a datetime (por si acaso)
        df[columna_fecha] = pd.to_datetime(df[columna_fecha], errors='coerce').dt.floor('D')

        print(f"✅ Se cambiaron {cambios} renglones en la columna '{columna_fecha}'.")
        return df

    @staticmethod
    def open_xlsx_file(file_path):
        """Abre un archivo XLSX con Excel si la ruta termina en .xlsx"""
        if not file_path.lower().endswith('.xlsx'):
            print(f"Error: El archivo no es un XLSX válido: {file_path}")
            return
        try:
            if os.name == 'nt':  # Windows
                os.startfile(file_path)
            elif os.name == 'posix':  # macOS o Linux
                if "darwin" in os.uname().sysname.lower():  # macOS
                    subprocess.run(["open", file_path])
                else:  # Linux
                    subprocess.run(["xdg-open", file_path])
        except Exception as e:
            print(f"Error opening XLSX file: {e}")


    @staticmethod
    def install_chromedriver():
        """Guide user to install Chrome and ChromeDriver for Testing into home directory."""

        import os
        import platform
        import zipfile
        import glob
        import subprocess
        import stat

        system = platform.system()
        home = os.path.expanduser("~")

        chrome_relative_parts = []

        if system == "Windows":
            target_dir = os.path.join(home, "Documents")
            chromedriver_prefix = "chromedriver-win64"
            chrome_prefix = "chrome-win64"
            chromedriver_exe = "chromedriver.exe"
            chrome_relative_parts = [chrome_prefix, "chrome.exe"]
        elif system == "Darwin":  # macOS
            target_dir = os.path.join(home, "chrome_testing")
            machine = platform.machine().lower()
            arch_suffix = "arm64" if "arm" in machine else "x64"
            chromedriver_prefix = f"chromedriver-mac-{arch_suffix}"
            chrome_prefix = f"chrome-mac-{arch_suffix}"
            chromedriver_exe = "chromedriver"
            chrome_relative_parts = [
                chrome_prefix,
                "Google Chrome for Testing.app",
                "Contents",
                "MacOS",
                "Google Chrome for Testing",
            ]
        else:
            print(f"❌ Unsupported OS: {system}")
            return None, None

        os.makedirs(target_dir, exist_ok=True)

        chrome_binary_path = os.path.join(target_dir, *chrome_relative_parts)
        chromedriver_path = os.path.join(target_dir, chromedriver_prefix, chromedriver_exe)

        if os.path.exists(chrome_binary_path) and os.path.exists(chromedriver_path):
            print("✅ Chrome for Testing ya está instalado. Reutilizando binarios existentes.")
            print(f"   Chrome: {chrome_binary_path}")
            print(f"   Chromedriver: {chromedriver_path}")
            return chrome_binary_path, chromedriver_path

        print("🌐 Open the following link to download the Chrome for Testing binaries:")
        print("👉 https://googlechromelabs.github.io/chrome-for-testing/\n")
        print(f"📂 Copia los zip en: {target_dir}")
        print("⬇️ Se requieren los archivos chromedriver y chrome del mismo release.")

        # Open folder for user convenience (macOS Finder / Windows Explorer)
        try:
            if system == "Darwin":
                subprocess.run(["open", target_dir], check=False)
            elif system == "Windows":
                subprocess.run(["explorer", target_dir], check=False)
        except Exception as e:
            print(f"(⚠️ No se pudo abrir la carpeta automáticamente: {e})")

        input("Presiona Enter cuando los zip estén en la carpeta...")

        chromedriver_zip = glob.glob(os.path.join(target_dir, f"{chromedriver_prefix}*.zip"))
        chrome_zip = glob.glob(os.path.join(target_dir, f"{chrome_prefix}*.zip"))

        # Recheck in case the user extracted manually before providing the zip
        if os.path.exists(chrome_binary_path) and os.path.exists(chromedriver_path):
            print("✅ Se detectó una instalación existente durante la verificación.")
            return chrome_binary_path, chromedriver_path

        if not chromedriver_zip or not chrome_zip:
            print("❌ No se encontraron los zip. Verifica los nombres descargados y vuelve a intentarlo.")
            return None, None

        chromedriver_zip = chromedriver_zip[0]
        chrome_zip = chrome_zip[0]

        def unzip_to_target(zip_path):
            with zipfile.ZipFile(zip_path, "r") as zip_ref:
                zip_ref.extractall(target_dir)
            print(f"✅ Se extrajo {os.path.basename(zip_path)}")

        unzip_to_target(chromedriver_zip)
        unzip_to_target(chrome_zip)

        if system == "Darwin":
            app_root = os.path.join(target_dir, chrome_prefix, "Google Chrome for Testing.app")
            quarantine_targets = [
                os.path.join(target_dir, chromedriver_prefix, chromedriver_exe),
                app_root,
            ]
            for target in quarantine_targets:
                try:
                    subprocess.run(
                        ["xattr", "-d", "com.apple.quarantine", target],
                        check=False,
                        stdout=subprocess.DEVNULL,
                        stderr=subprocess.DEVNULL,
                    )
                except Exception as e:
                    print(f"(⚠️ No se pudo limpiar quarantine en {target}: {e})")

            def ensure_executable(path):
                if os.path.exists(path):
                    try:
                        mode = os.stat(path).st_mode
                        if not (mode & stat.S_IXUSR):
                            os.chmod(
                                path,
                                mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH,
                            )
                    except Exception as exec_err:
                        print(f"(⚠️ No se pudo ajustar permisos en {path}: {exec_err})")

            ensure_executable(chromedriver_path)
            ensure_executable(chrome_binary_path)

            helpers_glob = glob.glob(
                os.path.join(
                    app_root,
                    "Contents",
                    "Frameworks",
                    "Google Chrome for Testing Framework.framework",
                    "Versions",
                    "*",
                    "Helpers",
                )
            )

            for helpers_dir in helpers_glob:
                for root, _dirs, files in os.walk(helpers_dir):
                    for filename in files:
                        ensure_executable(os.path.join(root, filename))
        else:
            for executable_path in [chromedriver_path, chrome_binary_path]:
                if os.path.exists(executable_path):
                    try:
                        current_mode = os.stat(executable_path).st_mode
                        if not (current_mode & stat.S_IXUSR):
                            os.chmod(
                                executable_path,
                                current_mode
                                | stat.S_IXUSR
                                | stat.S_IXGRP
                                | stat.S_IXOTH,
                            )
                    except Exception as e:
                        print(f"(⚠️ No se pudo ajustar permisos en {executable_path}: {e})")

        for zip_path in [chromedriver_zip, chrome_zip]:
            try:
                os.remove(zip_path)
            except Exception as e:
                print(f"(⚠️ No se pudo borrar {zip_path}: {e})")

        if not os.path.exists(chromedriver_path) or not os.path.exists(chrome_binary_path):
            print("❌ Hubo un problema al preparar los binarios. Revisa los archivos extraídos.")
            return None, None

        print(f"✅ Chromedriver listo en: {chromedriver_path}")
        print(f"✅ Chrome for Testing listo en: {chrome_binary_path}")

        return chrome_binary_path, chromedriver_path

    @staticmethod
    def chrome_driver_load(directory):
        """Launch Chrome with OS-specific paths and consistent configuration."""

        import os
        import platform
        from selenium import webdriver
        from selenium.common.exceptions import SessionNotCreatedException, WebDriverException
        from selenium.webdriver.chrome.service import Service
        from selenium.webdriver.chrome.options import Options

        os.makedirs(os.path.abspath(directory), exist_ok=True)

        chrome_binary_path, chromedriver_path = Helper.install_chromedriver()
        if not chrome_binary_path or not chromedriver_path:
            print("❌ No fue posible obtener los binarios de Chrome.")
            return None

        system = platform.system()

        chrome_options = Options()
        chrome_options.binary_location = chrome_binary_path

        prefs = {
            "download.default_directory": os.path.abspath(directory),
            "download.prompt_for_download": False,
            "download.directory_upgrade": True,
            "plugins.always_open_pdf_externally": True,
        }
        chrome_options.add_experimental_option("prefs", prefs)
        chrome_options.add_experimental_option("excludeSwitches", ["enable-logging"])
        chrome_options.add_argument("--window-size=1920x1080")
        chrome_options.add_argument("--remote-allow-origins=*")

        if system == "Linux":
            chrome_options.add_argument("--disable-dev-shm-usage")
            chrome_options.add_argument("--no-sandbox")
        elif system == "Windows":
            chrome_options.add_argument("--disable-gpu")

        try:
            service = Service(chromedriver_path)
            driver = webdriver.Chrome(service=service, options=chrome_options)
            print("🚀 ChromeDriver launched successfully.")
            return driver
        except SessionNotCreatedException as exc:
            print(f"❌ ChromeDriver no pudo crear la sesión: {exc}")
        except WebDriverException as exc:
            print(f"❌ Error al iniciar ChromeDriver: {exc}")
        except Exception as exc:
            print(f"❌ Error inesperado al iniciar ChromeDriver: {exc}")

        return None

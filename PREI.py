import os
import time
import datetime
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.keys import Keys
from selenium.common.exceptions import TimeoutException
import os
import time
import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
import datetime
import glob
import platform
import openpyxl
from downloaded_files_manager import DownloadedFilesManager

class PREI_MANAGEMENT:
    def __init__(self, working_folder, web_driver_manager, data_access):
        self.working_folder = working_folder
        self.web_driver_manager = web_driver_manager
        self.data_access = data_access

    def descargar_PREI(self, PREI_path):
        os.makedirs(PREI_path, exist_ok=True)
        username = self.data_access['PREI_user']
        password = self.data_access['PREI_password']
        sub_path = os.path.abspath(os.path.join(PREI_path, ".."))
        excel_path = {
            2023: os.path.join(sub_path, "2023_dates.xlsx"),
            2024: os.path.join(sub_path, "2024_dates.xlsx"),
            2025: os.path.join(sub_path, "2025_dates.xlsx")
        }

        # Actualiza último DATE END del año actual si no es hoy
        today_str = datetime.datetime.today().strftime('%d/%m/%Y')
        current_year = datetime.datetime.today().year
        if current_year in excel_path and os.path.exists(excel_path[current_year]):
            try:
                df_year = pd.read_excel(excel_path[current_year])
                df_year_clean = df_year.dropna(subset=['DATE START', 'DATE END'])
                if not df_year_clean.empty:
                    last_idx = df_year_clean.index[-1]
                    if str(df_year_clean.loc[last_idx, 'DATE END']).strip() != today_str:
                        df_year.loc[last_idx, 'DATE END'] = today_str
                        df_year.to_excel(excel_path[current_year], index=False)
                        print(f"Actualizando fecha del PREI {current_year}")
            except Exception as e:
                print(f"No se pudo actualizar el Excel del {current_year}: {e}")

        # Crear un driver por archivo para evitar cierres inesperados entre años
        overall = True
        for year, path in excel_path.items():
            if not os.path.exists(path):
                print(f"Excel no encontrado para {year}: {path}")
                continue
            driver = self.web_driver_manager.create_driver(PREI_path)
            try:
                ok = self.PREI_downloader(driver, username, password, PREI_path, path)
                overall = overall and ok
            finally:
                try:
                    driver.quit()
                except Exception:
                    pass
        return overall

    def PREI_downloader_noquit(self, driver, username, password, download_directory, excel_file):
        """Igual a PREI_downloader pero sin cerrar el driver, para procesar múltiples Excels seguidos."""
        df_fecha = pd.read_excel(excel_file)
        df_fecha = df_fecha.dropna(subset=['DATE START', 'DATE END'])

        print(f"Procesando {len(df_fecha)} rangos de fechas")

        df_missing = self.check_missing_files(df_fecha, username, download_directory)

        if df_missing.empty:
            print("All files are present and valid.")
            return True
        else:
            print(f"Descargando {len(df_missing)} archivos faltantes:")
            for _, row in df_missing.iterrows():
                print(f"Downloading: {self.convert_date_format(row['DATE START'])} to {self.convert_date_format(row['DATE END'])}")

            self.download_files(driver, df_missing, username, password)

            print("Verificando si todas las descargas se completaron...")
            df_still_missing = self.check_missing_files(df_fecha, username, download_directory)

            if df_still_missing.empty:
                print("Todas las descargas se completaron exitosamente")
                return True
            else:
                print(f"Aún faltan {len(df_still_missing)} archivos por descargar")
                print("Puedes ejecutar nuevamente para completar las descargas pendientes")
                return False

    def convert_date_format(self, date):
        return date.replace("/", "-")

    def clear_input_field(self, driver, xpath):
        """Ensure the input field is completely cleared."""
        for attempt in range(5):  # Attempt up to 5 times
            try:
                # Re-locate the element to ensure it's current
                input_field = driver.find_element(By.XPATH, xpath)
                # Clear the field using keys
                input_field.send_keys(Keys.CONTROL, 'a')  # Select all text
                time.sleep(0.2)
                input_field.send_keys(Keys.DELETE)  # Delete selected text
                time.sleep(0.2)
                # Get the current value using get_attribute and JavaScript
                current_value = input_field.get_attribute('value')
                js_value = driver.execute_script("return arguments[0].value;", input_field)
                #print(f"Attempt {attempt + 1}: Current value (via JS): '{js_value}'")
                # If either method indicates the field is cleared, exit successfully
                if current_value == '__/__/____' or js_value == '__/__/____':
                    print(f"Field cleared successfully on attempt {attempt + 1}")
                    return True
            except Exception as e:
                print(f"Attempt {attempt + 1}: Failed to clear input field. Error: {e}")
                time.sleep(1)  # Small delay before retrying
        raise TimeoutException("Failed to clear the input field after multiple attempts.")

    def input_date(self, driver, input_field_xpath, date):
        """Clear the input field and input the new date."""
        try:
            print(f"Date passed: {date}")
            actions = ActionChains(driver)
            # Locate the input field and click it
            input_field = driver.find_element(By.XPATH, input_field_xpath)
            actions.click(input_field).perform()
            time.sleep(0.2)
            # Close the calendar popup if it appears
            actions.send_keys(Keys.ESCAPE).perform()
            time.sleep(0.2)
            # Ensure the field is cleared
            self.clear_input_field(driver, input_field_xpath)
            time.sleep(0.2)
            # Convert the date to a string and input it
            date_str = str(date)
            print(f"Attempting to input date: {date_str}")
            input_field = driver.find_element(By.XPATH, input_field_xpath)  # Re-locate before sending keys
            input_field.send_keys(date_str)
            #print(f"Date '{date_str}' entered successfully into the field.")
            time.sleep(0.5)
        except Exception as e:
            print(f"Error in input_date for field '{input_field_xpath}' with date '{date}': {e}")
            raise

    def download_files(self, driver, df, username, password):
        """
        Uses the provided driver to log into the PREI system and process each date range.
        """
        # Define XPaths for all required elements
        elements_xpaths = {
            'close_button': "/html/body/div[2]/div[1]/a",
            'User': "/html/body/main/div[1]/div[2]/div[2]/form/div[2]/div[1]/div/input",
            'Password': "/html/body/main/div[1]/div[2]/div[2]/form/div[2]/div[2]/div[1]/input",
            'Login_button': "/html/body/main/div[1]/div[2]/div[2]/form/div[2]/div[3]/div/button[1]",
            'fecha_inicial': "/html/body/main/div[3]/div/div/form/div[2]/div[3]/span/div[1]/span[2]/input",
            'fecha_final': "/html/body/main/div[3]/div/div/form/div[2]/div[3]/span/div[2]/span[2]/input",
            'buscar': "/html/body/main/div[3]/div/div/form/div[2]/div[4]/div[1]/button/span",
            'excel': "/html/body/main/div[3]/div/div/form/div[5]/a/img",
            'alerta': "/html/body/main/div[3]/div/div/form/div[3]",
            'menu_pagos': "/html/body/main/nav/div/div[2]/form/div/ul/li[2]/a/span[1]",
            'no_results': "/html/body/main/div[3]/div/div/form/div[4]/div[2]/table/tbody/tr/td",
            'facturasvscr': "/html/body/main/nav/div/div[2]/form/div/ul/li[2]/ul/li[6]/a/span"
        }

        # Navigate to the PREI login page
        driver.get('https://pispdigital.imss.gob.mx/piref/')
        time.sleep(2)

        # Login Process using the provided credentials
        WebDriverWait(driver, 30).until(EC.element_to_be_clickable((By.XPATH, elements_xpaths['close_button'])))
        driver.find_element(By.XPATH, elements_xpaths['close_button']).click()
        WebDriverWait(driver, 30).until(EC.element_to_be_clickable((By.XPATH, elements_xpaths['Login_button'])))
        driver.find_element(By.XPATH, elements_xpaths['User']).send_keys(username)
        driver.find_element(By.XPATH, elements_xpaths['Password']).send_keys(password)
        driver.find_element(By.XPATH, elements_xpaths['Login_button']).click()
        time.sleep(1)

        # Navigate to the 'facturasvscr' section after login
        menu_pagos_element = WebDriverWait(driver, 30).until(
            EC.visibility_of_element_located((By.XPATH, elements_xpaths['menu_pagos']))
        )
        actions = ActionChains(driver)
        actions.move_to_element(menu_pagos_element).perform()
        time.sleep(2)
        WebDriverWait(driver, 30).until(EC.element_to_be_clickable((By.XPATH, elements_xpaths['facturasvscr']))).click()
        time.sleep(3)

        # Loop through each row (date range) in the DataFrame
        for index, row in df.iterrows():
            try:
                WebDriverWait(driver, 30).until(
                    EC.visibility_of_element_located((By.XPATH, elements_xpaths['fecha_inicial']))
                )
                WebDriverWait(driver, 30).until(
                    EC.visibility_of_element_located((By.XPATH, elements_xpaths['fecha_final']))
                )

                print(f"Processing row {index + 1}: Date START = {row['DATE START']}, Date END = {row['DATE END']}")
                print("Calling input_date for fecha_inicial...")
                self.input_date(driver, elements_xpaths['fecha_inicial'], row['DATE START'])
                print("fecha_inicial set successfully.")

                print("Calling input_date for fecha_final...")
                self.input_date(driver, elements_xpaths['fecha_final'], row['DATE END'])
                print("fecha_final set successfully.")

                print("Clicking 'buscar' button...")
                WebDriverWait(driver, 30).until(EC.element_to_be_clickable((By.XPATH, elements_xpaths['buscar']))).click()
                time.sleep(4)

                alerta_element = driver.find_element(By.XPATH, elements_xpaths['alerta'])
                if "Se encontraron más de 100 coincidencias" in alerta_element.text:
                    print(f"Dates {row['DATE START']} to {row['DATE END']} got more than 100 invoices, please modify")
                    continue  # Skip to the next row

                excel_element = WebDriverWait(driver, 30).until(EC.element_to_be_clickable((By.XPATH, elements_xpaths['excel'])))
                no_results_elements = driver.find_elements(By.XPATH, elements_xpaths['no_results'])

                if len(no_results_elements) > 0 and no_results_elements[0].text == "No se encontraron resultados.":
                    print(f"Dates {row['DATE START']} to {row['DATE END']} got no results, moving to the next set.")
                    continue  # Skip to the next row
                elif excel_element:
                    try:
                        # Wait until any overlay disappears
                        overlay_xpath = "//*[@id='j_idt26_modal']"
                        WebDriverWait(driver, 30).until(EC.invisibility_of_element_located((By.XPATH, overlay_xpath)))
                    except TimeoutException:
                        print("Overlay did not disappear, refreshing the page...")
                        driver.refresh()
                        continue  # Skip to the next row

                    excel_element = WebDriverWait(driver, 30).until(EC.element_to_be_clickable((By.XPATH, elements_xpaths['excel'])))
                    excel_element.click()
                    time.sleep(2)
                    print(f"Dates {row['DATE START']} to {row['DATE END']} were processed")
                else:
                    print(f"Without results for {row['DATE START']} to {row['DATE END']}.")
            except Exception as e:
                print(f"An error occurred: {e}")
            finally:
                print("Moving to the next set.")
                driver.execute_script("window.scrollTo(0, 0);")
    def clean_download_directory(self, download_directory):
        """
        Removes .xls files not created today by checking file system metadata.
        Returns a set of remaining file names (basename).
        """
        valid_files = set()
        today = datetime.date.today()
        
        # Only look for .xls files (as you mentioned these are the expected files)
        xls_files = glob.glob(os.path.join(download_directory, "*.xls"))
        
        # Also remove any .xlsx files (these shouldn't be here)
        xlsx_files = glob.glob(os.path.join(download_directory, "*.xlsx"))
        for xlsx_file in xlsx_files:
            print(f"🗑️ Removing unexpected .xlsx file: {os.path.basename(xlsx_file)}")
            try:
                os.remove(xlsx_file)
            except Exception as e:
                print(f"   ❌ Error removing .xlsx file: {e}")
        
        print(f"🔍 Found {len(xls_files)} .xls files to check")

        for file_path in xls_files:
            file_name = os.path.basename(file_path)
            should_keep = False
            
            try:
                # Método: Usar solo fecha de modificación del sistema de archivos
                file_stats = os.stat(file_path)
                mod_timestamp = file_stats.st_mtime
                mod_date = datetime.date.fromtimestamp(mod_timestamp)
                
                print(f"📅 {file_name} - File Modified: {mod_date} (Today: {today})")
                
                # Solo mantener archivos modificados HOY
                should_keep = (mod_date == today)
                
                # Decision: keep or remove
                if should_keep:
                    valid_files.add(file_name)
                    print(f"✅ Keeping: {file_name}")
                else:
                    print(f"🗑️ Removing {file_name} (modified: {mod_date}, not today)")
                    os.remove(file_path)
                    
            except Exception as e:
                print(f"❌ Error processing {file_name}: {e}")
                # If we can't determine the date, remove it to be safe
                try:
                    os.remove(file_path)
                    print(f"🗑️ Removed {file_name} (couldn't determine date)")
                except:
                    pass
        
        print(f"📊 Remaining .xls files: {len(valid_files)}")
        return valid_files
    

    def check_missing_files(self, df, username, download_directory):
        """
        Checks for missing files based on the date ranges provided in df.
        Prior to checking, cleans the download directory by removing invalid files.
        Returns a DataFrame with the rows corresponding to missing files.
        """
        valid_files = self.clean_download_directory(download_directory)
        missing_rows = []
        expected_files = []
        for index, row in df.iterrows():
            date_start = self.convert_date_format(row['DATE START'])
            date_end = self.convert_date_format(row['DATE END'])
            file_name = f'[FacturaVsCR][{username}][{date_start}][{date_end}].xls'
            expected_files.append(file_name)
            # Only add row if the expected file is not among the valid files
            if file_name not in valid_files:
                missing_rows.append(row)
        # 🗑️ Remove files that are valid but not expected
        files_to_remove = set(valid_files) - set(expected_files)
        
        if False and files_to_remove:
            print(f"🗑️ Eliminando {len(files_to_remove)} archivos no esperados:")
            for file_to_remove in files_to_remove:
                file_path = os.path.join(download_directory, file_to_remove)
                try:
                    os.remove(file_path)
                    print(f"   ✅ Eliminado: {file_to_remove}")
                except Exception as e:
                    print(f"   ❌ Error eliminando {file_to_remove}: {e}")
        else:
            print("✅ No hay archivos no esperados para eliminar")
        
        # 📊 Report final status
        remaining_expected = set(expected_files) & set(valid_files)
        print(f"📊 Archivos esperados restantes: {len(remaining_expected)}")
        print(f"📊 Archivos faltantes: {len(missing_rows)}")
        
        return pd.DataFrame(missing_rows)

    def PREI_downloader(self, driver, username, password, download_directory, excel_file):
        """
        Executes the PREI downloader process.
        
        Steps:
        1. Read the Excel file to get date ranges.
        2. Clean the download directory and check which files are missing/invalid.
        3. Download files for the missing date ranges.
        4. Re-check to confirm all files are now present.
        """
        # Limpiar datos del Excel (eliminar filas vacías/nulas)
        df_fecha = pd.read_excel(excel_file)
        df_fecha = df_fecha.dropna(subset=['DATE START', 'DATE END'])  # ✅ Eliminar filas con fechas vacías
        
        print(f"📅 Procesando {len(df_fecha)} rangos de fechas")
        
        # Primera verificación
        df_missing = self.check_missing_files(df_fecha, username, download_directory)
        
        if df_missing.empty:
            print("✅ All files are present and valid.")
            driver.quit()  # Cerrar driver si no hay nada que hacer
            return True
        else:
            max_retries = 5  # Maximum number of download attempts to avoid infinite loops
            attempt = 0
            df_still_missing = df_missing
            
            while not df_still_missing.empty and attempt < max_retries:
                attempt += 1
                print(f"📥 Intento {attempt}/{max_retries}: Descargando {len(df_still_missing)} archivos faltantes:")
                for index, row in df_still_missing.iterrows():
                    print(f"⬇️ Downloading: {self.convert_date_format(row['DATE START'])} to {self.convert_date_format(row['DATE END'])}")
                
                # Descargar archivos faltantes
                self.download_files(driver, df_still_missing, username, password)
                
                # ✅ VERIFICACIÓN - Re-check después de descargar
                print(f"\n🔍 Verificando después del intento {attempt}...")
                df_still_missing = self.check_missing_files(df_fecha, username, download_directory)
                
                if df_still_missing.empty:
                    print("✅ Todas las descargas se completaron exitosamente")
                    driver.quit()
                    return True
                else:
                    print(f"⚠️ Intento {attempt}: Aún faltan {len(df_still_missing)} archivos por descargar")
                    if attempt < max_retries:
                        print("🔄 Reintentando automáticamente...")
                    else:
                        print(f"❌ Máximo de intentos ({max_retries}) alcanzado. Archivos faltantes:")
                        for index, row in df_still_missing.iterrows():
                            print(f"   - {self.convert_date_format(row['DATE START'])} to {self.convert_date_format(row['DATE END'])}")
                        print("🔄 Puedes ejecutar nuevamente para completar las descargas pendientes o revisar manualmente.")
            
            # If we exit the loop without success, quit and return False
            driver.quit()
            return False

    """ 
    def main():
        downloads_path = os.path.join(self.working_folder)
        self.web_driver_manager = WebAutomationDriver(downloads_path)
        self.data_access = self.config_manager.yaml_creation(self.working_folder)
        
        if self.data_access is None:
            print("⚠️ Configura el archivo YAML antes de continuar")
            return False
        
        # Inicializar web driver manager (sin crear el driver aún)
        downloads_path = os.path.join(self.working_folder)
        self.web_driver_manager = WebAutomationDriver(downloads_path)
        # Inicializar SAI manager
        self.prei_manager = PREI_MANAGEMENT(self.working_folder, self.web_driver_manager, self.data_access)

if __name__ == "__main__":
    main()
    """

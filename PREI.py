import os
import time
import datetime
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.keys import Keys
from selenium.common.exceptions import TimeoutException
from helpers import create_directory_if_not_exists
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
class PREI_MANAGEMENT:
    def __init__(self, working_folder, web_driver_manager, data_access):
        self.working_folder = working_folder
        self.web_driver_manager = web_driver_manager
        self.data_access = data_access

    def descargar_PREI(self, PREI_path):
        create_directory_if_not_exists(PREI_path)
        username = self.data_access['PREI_user']
        password = self.data_access['PREI_password']
        excel_path = os.path.join(PREI_path, "2025_dates.xlsx")
        temporal_PREI_path = os.path.join(PREI_path, "Temporal downloads")
        create_directory_if_not_exists(temporal_PREI_path)
        driver = self.web_driver_manager.create_driver(temporal_PREI_path)
        exito_prei = self.PREI_downloader(driver, username, password, temporal_PREI_path, excel_path)
        return exito_prei

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

    def clean_download_directory(self, download_directory, username):
        """
        Cleans the download_directory by removing .xls files that are either:
        - Not created today, or
        - Not loadable as a non-empty DataFrame.
        Returns a set of valid file names (basename) present in the directory.
        """
        valid_files = set()
        today = datetime.date.today()
        # List all .xls files in the directory
        xls_files = glob.glob(os.path.join(download_directory, "*.xls"))
        
        for file_path in xls_files:
            try:
                # Get file creation time as date
                file_creation_date = datetime.date.fromtimestamp(os.path.getctime(file_path))
            except Exception as e:
                print(f"Error getting creation time for {os.path.basename(file_path)}: {e}")
                continue
            
            if file_creation_date != today:
                print(f"remove {os.path.basename(file_path)} (not from today)")
                os.remove(file_path)
                continue
            
            # Check if file is valid (loadable and non-empty)
            try:
                df_file = pd.read_excel(file_path)
                if df_file.empty:
                    print(f"remove {os.path.basename(file_path)} (empty file)")
                    os.remove(file_path)
                    continue
            except Exception as e:
                print(f"remove {os.path.basename(file_path)} (not loadable: {e})")
                os.remove(file_path)
                continue
            
            # File passed the checks: add its basename to the set
            valid_files.add(os.path.basename(file_path))
        
        return valid_files

    def check_missing_files(self, df, username, download_directory):
        """
        Checks for missing files based on the date ranges provided in df.
        Prior to checking, cleans the download directory by removing invalid files.
        Returns a DataFrame with the rows corresponding to missing files.
        """
        valid_files = self.clean_download_directory(download_directory, username)
        missing_rows = []
        
        for index, row in df.iterrows():
            date_start = self.convert_date_format(row['DATE START'])
            date_end = self.convert_date_format(row['DATE END'])
            file_name = f'[FacturaVsCR][{username}][{date_start}][{date_end}].xls'
            # Only add row if the expected file is not among the valid files
            if file_name not in valid_files:
                missing_rows.append(row)
        
        return pd.DataFrame(missing_rows)

    def PREI_downloader(self, driver, username, password, download_directory, excel_file):
        """
        Executes the PREI downloader process.
        
        Steps:
        1. Read the Excel file to get date ranges.
        2. Clean the download directory and check which files are missing/invalid.
        3. Download files for the missing date ranges.
        """
        """
        df = pd.read_excel(excel_file)
        # Remove invalid or outdated files and get missing date ranges
        df_missing = check_missing_files(df, username, download_directory)
        if df_missing.empty:
            print("All files are present and valid.")
        else:
            #print("Missing or invalid files for the following date ranges:")
            #print(df_missing.head(20))
            for index, row in df_missing.iterrows():
                print(f"{convert_date_format(row['DATE START'])} to {convert_date_format(row['DATE END'])}")
            # Attempt to download the missing files
            download_files(driver, df_missing, username, password)
        """
        df = pd.read_excel(excel_file)
        df_missing = self.check_missing_files(df, username, download_directory)
        if df_missing.empty:
            print("✅ All files are present and valid.")
            return True
        else:
            for index, row in df_missing.iterrows():
                print(f"⬇️ Downloading: {self.convert_date_format(row['DATE START'])} to {self.convert_date_format(row['DATE END'])}")
            self.download_files(driver, df_missing, username, password)
            return False
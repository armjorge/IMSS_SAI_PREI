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


class SAI_MANAGEMENT:
    def __init__(self, working_folder, web_driver_manager, data_access):
        self.working_folder = working_folder
        self.web_driver_manager = web_driver_manager
        self.data_access = data_access

        # XPaths centralizados (Altas y Ordenes)
        self.elements_xpaths = {
            # Login
            'user': "/html/body/main/div[2]/app-root/app-autenticacion/div[3]/form/div[1]/input",
            'password': "/html/body/main/div[2]/app-root/app-autenticacion/div[3]/form/div[2]/input",
            'Login_button': "/html/body/main/div[2]/app-root/app-autenticacion/div[3]/form/button",
            # Menú principal desde Home
            'Menu': "/html/body/main/div[2]/app-root/app-home/app-header/nav/div/div/ul[2]/li/a",
            'Altas': "/html/body/main/div[2]/app-root/app-home/app-header/nav/div/div/ul[2]/li/ul/li[6]/a",
            # Una vez en Altas, header cambia
            'Menu_ordenes': "/html/body/main/div[2]/app-root/app-altas/app-header/nav/div/div/ul[2]/li/a",
            'Ordenes': "/html/body/main/div[2]/app-root/app-altas/app-header/nav/div/div/ul[2]/li/ul/li[3]/a",
            # Inputs y botones Altas
            'Altas_inicial': "/html/body/main/div[2]/app-root/app-altas/div[1]/form/div[6]/div[7]/div/input",
            'Altas_final': "/html/body/main/div[2]/app-root/app-altas/div[1]/form/div[6]/div[8]/div/input",
            'Altas_consultar': "/html/body/main/div[2]/app-root/app-altas/div/div[2]/div[2]/button[2]",
            'Altas_exportar': "/html/body/main/div[2]/app-root/app-altas/div[2]/div/button",
            # Inputs y botones Ordenes
            'Ordenes_inicial': "/html/body/main/div[2]/app-root/app-consulta-ordenes/div[3]/form/div[4]/div[3]/div/input",
            'Ordenes_final': "/html/body/main/div[2]/app-root/app-consulta-ordenes/div[3]/form/div[4]/div[4]/div/input",
            'Ordenes_consultar': "/html/body/main/div[2]/app-root/app-consulta-ordenes/div[3]/div[2]/div[2]/button[2]",
            'Ordenes_exportar': "/html/body/main/div[2]/app-root/app-consulta-ordenes/div[4]/div/button",
        }

    def descargar_altas(self, temporal_altas_path):
        # Para simplificar el flujo solicitado, reutilizamos el combinado
        # que descarga Altas y Ordenes por cada rango en una sola sesión.
        return self.descargar_altas_y_ordenes(temporal_altas_path)

    def descargar_ordenes(self, temporal_ordenes_path):
        create_directory_if_not_exists(temporal_ordenes_path)
        self.driver = self.web_driver_manager.create_driver(temporal_ordenes_path)
        self.username = self.data_access['SAI_user']
        self.password = self.data_access['SAI_password']
        self.today = datetime.datetime.today().strftime('%d/%m/%Y')

        if not hasattr(self, 'range_date_multi'):
            self.range_date_multi = {
                2023: ['01/01/2023', '31/12/2023'],
                2024: ['01/01/2024', '31/12/2024'],
                2025: ['01/01/2025', self.today],
            }

        print("Iniciando descarga Ordenes (multi-rango)...")
        return self._sai_download_ordenes(temporal_ordenes_path)

    def _clear_and_type_date(self, input_element, value):
        """Limpia robustamente el input y escribe la fecha, validando el valor."""
        try:
            input_element.click()
            time.sleep(0.2)
            input_element.send_keys(Keys.ESCAPE)
            time.sleep(0.2)
            input_element.send_keys(Keys.CONTROL, 'a')
            time.sleep(0.1)
            input_element.send_keys(Keys.DELETE)
            time.sleep(0.2)
            input_element.send_keys(value)
            time.sleep(0.3)

            # Validar que quedó escrito
            current = input_element.get_attribute('value') or ''
            if current.strip() != value:
                self.driver.execute_script(
                    "arguments[0].value = arguments[1]; arguments[0].dispatchEvent(new Event('input'));",
                    input_element,
                    value,
                )
        except Exception:
            # Fallback directo por JS
            try:
                self.driver.execute_script(
                    "arguments[0].value = arguments[1]; arguments[0].dispatchEvent(new Event('input'));",
                    input_element,
                    value,
                )
            except Exception:
                pass

    def _login_and_open_altas(self):
        driver = self.driver
        xp = self.elements_xpaths

        print("[1] Abriendo URL Altas...")
        driver.get('https://ppsai-abasto.imss.gob.mx/abasto-web/reporteAltas')
        time.sleep(2)

        print("[2] Detectando iframes (previo captcha)...")
        WebDriverWait(driver, 10).until(
            EC.presence_of_all_elements_located((By.TAG_NAME, "iframe"))
        )
        _ = driver.find_elements(By.TAG_NAME, "iframe")
        input("Valida el captcha y presiona ENTER...")

        driver.switch_to.default_content()

        print("[3] Login...")
        user_field = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.XPATH, xp['user']))
        )
        user_field.clear()
        user_field.send_keys(self.username)

        password_field = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.XPATH, xp['password']))
        )
        password_field.clear()
        password_field.send_keys(self.password)

        driver.find_element(By.XPATH, xp['Login_button']).click()
        print("Login enviado.")

        action = ActionChains(driver)
        menu_element = WebDriverWait(driver, 30).until(
            EC.visibility_of_element_located((By.XPATH, xp['Menu']))
        )
        action.move_to_element(menu_element).click().perform()
        time.sleep(1)
        WebDriverWait(driver, 30).until(
            EC.element_to_be_clickable((By.XPATH, xp['Altas']))
        ).click()
        time.sleep(1)

    def _sai_download_altas(self, download_path):
        driver = self.driver
        xp = self.elements_xpaths

        initial_files = [f for f in os.listdir(download_path) if os.path.isfile(os.path.join(download_path, f))]
        initial_count = len(initial_files)
        session_start = time.time()

        try:
            self._login_and_open_altas()

            for year, (start_date, end_date) in self.range_date_multi.items():
                print(f"[Altas] Rango {year}: {start_date} a {end_date}")

                start_input = WebDriverWait(driver, 20).until(
                    EC.presence_of_element_located((By.XPATH, xp['Altas_inicial']))
                )
                end_input = WebDriverWait(driver, 20).until(
                    EC.presence_of_element_located((By.XPATH, xp['Altas_final']))
                )

                self._clear_and_type_date(start_input, start_date)
                time.sleep(0.4)
                self._clear_and_type_date(end_input, end_date)
                time.sleep(0.4)

                WebDriverWait(driver, 30).until(
                    EC.element_to_be_clickable((By.XPATH, xp['Altas_consultar']))
                ).click()
                time.sleep(2)

                export_btn = WebDriverWait(driver, 30).until(
                    EC.element_to_be_clickable((By.XPATH, xp['Altas_exportar']))
                )
                export_btn.click()

                try:
                    WebDriverWait(driver, 180).until_not(
                        EC.element_to_be_clickable((By.XPATH, xp['Altas_exportar']))
                    )
                except TimeoutException:
                    print("Aviso: botón Exportar (Altas) no cambió de estado.")

                WebDriverWait(driver, 300).until(
                    EC.element_to_be_clickable((By.XPATH, xp['Altas_exportar']))
                )
                time.sleep(2)

            final_files = [f for f in os.listdir(download_path) if os.path.isfile(os.path.join(download_path, f))]
            new_files = len(final_files) - initial_count
            expected = len(self.range_date_multi)
            if new_files >= expected:
                print(f"Altas: {new_files} archivo(s) nuevo(s). Archivos: {final_files}")
                return True
            else:
                print(f"Altas: esperados {expected} archivo(s), encontrados {new_files}")
                input("Revisa Descargas de Chrome. Si están los archivos, ENTER.")
                return False
        except Exception as e:
            print(f"Error en Altas: {e}")
            return False

    def _sai_download_ordenes(self, download_path):
        driver = self.driver
        xp = self.elements_xpaths

        initial_files = [f for f in os.listdir(download_path) if os.path.isfile(os.path.join(download_path, f))]
        initial_count = len(initial_files)

        try:
            # Reutilizamos login+Altas para tener el header y luego navegamos a Ordenes
            self._login_and_open_altas()

            menu_ord = WebDriverWait(driver, 30).until(
                EC.visibility_of_element_located((By.XPATH, xp['Menu_ordenes']))
            )
            ActionChains(driver).move_to_element(menu_ord).click().perform()
            time.sleep(1)
            WebDriverWait(driver, 30).until(
                EC.element_to_be_clickable((By.XPATH, xp['Ordenes']))
            ).click()
            time.sleep(1)

            for year, (start_date, end_date) in self.range_date_multi.items():
                print(f"[Ordenes] Rango {year}: {start_date} a {end_date}")

                start_input = WebDriverWait(driver, 20).until(
                    EC.presence_of_element_located((By.XPATH, xp['Ordenes_inicial']))
                )
                end_input = WebDriverWait(driver, 20).until(
                    EC.presence_of_element_located((By.XPATH, xp['Ordenes_final']))
                )

                self._clear_and_type_date(start_input, start_date)
                time.sleep(0.4)
                self._clear_and_type_date(end_input, end_date)
                time.sleep(0.4)

                WebDriverWait(driver, 30).until(
                    EC.element_to_be_clickable((By.XPATH, xp['Ordenes_consultar']))
                ).click()
                time.sleep(2)

                export_btn = WebDriverWait(driver, 100).until(
                    EC.element_to_be_clickable((By.XPATH, xp['Ordenes_exportar']))
                )
                export_btn.click()

                try:
                    WebDriverWait(driver, 300).until_not(
                        EC.element_to_be_clickable((By.XPATH, xp['Ordenes_exportar']))
                    )
                    time.sleep(2)
                except TimeoutException:
                    print("Aviso: botón Exportar (Ordenes) no cambió de estado.")

                WebDriverWait(driver, 600).until(
                    EC.element_to_be_clickable((By.XPATH, xp['Ordenes_exportar']))
                )
                time.sleep(2)

            final_files = [f for f in os.listdir(download_path) if os.path.isfile(os.path.join(download_path, f))]
            new_files = len(final_files) - initial_count
            expected = len(self.range_date_multi)
            if new_files >= expected:
                print(f"Ordenes: {new_files} archivo(s) nuevo(s). Archivos: {final_files}")
                return True
            else:
                print(f"Ordenes: esperados {expected} archivo(s), encontrados {new_files}")
                input("Revisa Descargas de Chrome. Si están los archivos, ENTER.")
                return False
        except Exception as e:
            print(f"Error en Ordenes: {e}")
            return False

    def descargar_altas_y_ordenes(self, download_path):
        """
        Descarga, por cada rango, Altas y luego Ordenes; valida 2 archivos por rango.
        Inicia sesión una vez. Usa el mismo directorio de descargas para ambos.
        """
        create_directory_if_not_exists(download_path)
        self.driver = self.web_driver_manager.create_driver(download_path)
        self.username = self.data_access['SAI_user']
        self.password = self.data_access['SAI_password']
        self.today = datetime.datetime.today().strftime('%d/%m/%Y')

        if not hasattr(self, 'range_date_multi'):
            self.range_date_multi = {
                2023: ['01/01/2023', '31/12/2023'],
                2024: ['01/01/2024', '31/12/2024'],
                2025: ['01/01/2025', self.today],
            }

        driver = self.driver
        xp = self.elements_xpaths

        def ensure_altas_page():
            try:
                WebDriverWait(driver, 5).until(
                    EC.presence_of_element_located((By.XPATH, xp['Altas_inicial']))
                )
            except Exception:
                driver.get('https://ppsai-abasto.imss.gob.mx/abasto-web/reporteAltas')
                WebDriverWait(driver, 20).until(
                    EC.presence_of_element_located((By.XPATH, xp['Altas_inicial']))
                )

        # Limpiar descargas previas para un conteo consistente
        try:
            for _f in os.listdir(download_path):
                _fp = os.path.join(download_path, _f)
                if os.path.isfile(_fp) and _f.lower().endswith(('.xlsx', '.xls', '.crdownload')):
                    try:
                        os.remove(_fp)
                    except Exception:
                        pass
        except Exception:
            pass
        # Conteo inicial
        initial_files = [f for f in os.listdir(download_path) if os.path.isfile(os.path.join(download_path, f))]
        initial_count = len(initial_files)

        try:
            # Login y abrir Altas una sola vez
            self._login_and_open_altas()

            total_expected = 0
            for year, (start_date, end_date) in self.range_date_multi.items():
                print(f"[Rango {year}] Iniciando Altas y Ordenes: {start_date} a {end_date}")

                # Asegurar que estamos en Altas
                ensure_altas_page()

                # Altas
                start_input = WebDriverWait(driver, 20).until(
                    EC.presence_of_element_located((By.XPATH, xp['Altas_inicial']))
                )
                end_input = WebDriverWait(driver, 20).until(
                    EC.presence_of_element_located((By.XPATH, xp['Altas_final']))
                )
                self._clear_and_type_date(start_input, start_date)
                time.sleep(0.4)
                self._clear_and_type_date(end_input, end_date)
                time.sleep(0.4)

                WebDriverWait(driver, 30).until(
                    EC.element_to_be_clickable((By.XPATH, xp['Altas_consultar']))
                ).click()
                time.sleep(2)
                WebDriverWait(driver, 30).until(
                    EC.element_to_be_clickable((By.XPATH, xp['Altas_exportar']))
                ).click()

                try:
                    WebDriverWait(driver, 180).until_not(
                        EC.element_to_be_clickable((By.XPATH, xp['Altas_exportar']))
                    )
                except TimeoutException:
                    print("Aviso: botón Exportar (Altas) no cambió de estado.")
                WebDriverWait(driver, 300).until(
                    EC.element_to_be_clickable((By.XPATH, xp['Altas_exportar']))
                )
                time.sleep(1.5)

                # Ir a Ordenes desde el header de Altas
                menu_ord = WebDriverWait(driver, 30).until(
                    EC.visibility_of_element_located((By.XPATH, xp['Menu_ordenes']))
                )
                ActionChains(driver).move_to_element(menu_ord).click().perform()
                time.sleep(1)
                WebDriverWait(driver, 30).until(
                    EC.element_to_be_clickable((By.XPATH, xp['Ordenes']))
                ).click()
                time.sleep(1)

                # Ordenes
                start_input_o = WebDriverWait(driver, 20).until(
                    EC.presence_of_element_located((By.XPATH, xp['Ordenes_inicial']))
                )
                end_input_o = WebDriverWait(driver, 20).until(
                    EC.presence_of_element_located((By.XPATH, xp['Ordenes_final']))
                )
                self._clear_and_type_date(start_input_o, start_date)
                time.sleep(0.4)
                self._clear_and_type_date(end_input_o, end_date)
                time.sleep(0.4)

                WebDriverWait(driver, 30).until(
                    EC.element_to_be_clickable((By.XPATH, xp['Ordenes_consultar']))
                ).click()
                time.sleep(2)
                WebDriverWait(driver, 100).until(
                    EC.element_to_be_clickable((By.XPATH, xp['Ordenes_exportar']))
                ).click()
                try:
                    WebDriverWait(driver, 300).until_not(
                        EC.element_to_be_clickable((By.XPATH, xp['Ordenes_exportar']))
                    )
                    time.sleep(1.5)
                except TimeoutException:
                    print("Aviso: botón Exportar (Ordenes) no cambió de estado.")
                WebDriverWait(driver, 600).until(
                    EC.element_to_be_clickable((By.XPATH, xp['Ordenes_exportar']))
                )
                time.sleep(1.0)

                # Validación por rango: +2 archivos
                total_expected += 2
                now_files = [f for f in os.listdir(download_path) if os.path.isfile(os.path.join(download_path, f))]
                new_files_so_far = len(now_files) - initial_count
                if new_files_so_far < total_expected:
                    print(f"Advertencia: tras rango {year} se esperaban {total_expected} archivos; hay {new_files_so_far}")
                else:
                    print(f"OK rango {year}: +2 archivos detectados. Total: {new_files_so_far}")

            # Validación final
            final_files = [f for f in os.listdir(download_path) if os.path.isfile(os.path.join(download_path, f))]
            total_new = len(final_files) - initial_count
            if total_new >= total_expected:
                print(f"Completado: {total_new} archivo(s) nuevos (esperados ≥ {total_expected}).")
                return True
            else:
                print(f"Faltan archivos: nuevos {total_new}, esperados {total_expected}.")
                input("Revisa Descargas de Chrome. Si están todos, ENTER para finalizar.")
                return False
        except Exception as e:
            print(f"Error en flujo combinado Altas+Ordenes: {e}")
            return False

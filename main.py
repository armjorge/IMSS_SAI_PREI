import os
from config import ConfigManager
from helpers import message_print, create_directory_if_not_exists
from web_automation_driver import WebAutomationDriver
from SAI import SAI_MANAGEMENT
from PREI import PREI_MANAGEMENT
from facturas_imss import FACTURAS_IMSS
from downloaded_files_manager import DownloadedFilesManager
from data_integration import DataIntegration
from sql_connexion_updating import SQL_CONNEXION_UPDATING
import pandas as pd

class MiniImssApp:
    def __init__(self):
        self.folder_root = os.getcwd()
        self.working_folder = os.path.join(self.folder_root, "Implementación")
        self.config_manager = ConfigManager(self.working_folder)
        self.web_driver = None
        self.data_access = None 
        self.integration_path = os.path.join(self.working_folder, "Integración")
        

    def initialize(self):
        """Inicializa los managers principales"""
        print("🚀 Inicializando aplicación...")
        
        # Inicializar configuración
        self.data_access = self.config_manager.yaml_creation(self.working_folder)
        
        if self.data_access is None:
            print("⚠️ Configura el archivo YAML antes de continuar")
            return False
        
        # Inicializar web driver manager (sin crear el driver aún)
        downloads_path = os.path.join(self.working_folder)
        self.web_driver_manager = WebAutomationDriver(downloads_path)
        # Inicializar SAI manager
        self.sai_manager = SAI_MANAGEMENT(self.working_folder, self.web_driver_manager, self.data_access)
        self.prei_manager = PREI_MANAGEMENT(self.working_folder, self.web_driver_manager, self.data_access)
        self.facturas_manager = FACTURAS_IMSS(self.working_folder, self.data_access)
        self.downloaded_files_manager = DownloadedFilesManager(self.working_folder, self.data_access)
        self.data_integration = DataIntegration(self.working_folder, self.data_access, self.integration_path)
        self.sql_integration = SQL_CONNEXION_UPDATING(self.working_folder, self.data_access)
        print("✅ Inicialización completada")
        return True

    def run(self):
        """Ejecuta el menú principal de la aplicación"""
        if not self.initialize():
            return
        altas_path = os.path.join(self.working_folder, "SAI")
        temporal_altas_path = os.path.join(altas_path, "Temporal downloads")
        create_directory_if_not_exists(temporal_altas_path)
        PREI_path = os.path.join(self.working_folder, "PREI")
        temporal_prei_path = os.path.join(PREI_path, 'Temporal downloads')
        create_directory_if_not_exists(temporal_prei_path)
        #ORDERS_processed_path = os.path.join(self.working_folder, "SAI", "Orders_Procesados")
        FACTURAS_processed_path = os.path.join(self.working_folder, "Facturas", "Consultas")
        PREI_processed_path = os.path.join(self.working_folder, "PREI", "PREI_files")
        ALTAS_processed_path = os.path.join(self.working_folder, "SAI", "SAI Altas_files")

        while True:
            print("\n" + "="*50)
            choice = input(message_print(
                "Elige una opción:\n"
                "\t1) Descargar altas\n"
                "\t2) Descargar PREI\n"
                "\t3) Cargar facturas\n"
                "\t4) Integrar información\n"
                "\t5) Actualizar SQL\n"
                "\t6) Ejecutar consultas SQL\n"
                "\tauto Ejecutar todo automáticamente\n"
                "\t0) Salir"
            )).strip()
        
            if choice == "1":
                exito_descarga_altas = self.sai_manager.descargar_altas(temporal_altas_path)
                if exito_descarga_altas:
                    print("✅ Descarga de Altas completada")
                    self.downloaded_files_manager.manage_downloaded_files(temporal_altas_path)
                else:
                    print("❌ Error en descarga de Altas")
            elif choice == "2":
                exito_descarga_prei = self.prei_manager.descargar_PREI(temporal_prei_path)
                if exito_descarga_prei:
                    print("✅ Descarga de PREI completada")
                    self.downloaded_files_manager.manage_downloaded_files(temporal_prei_path)
                else:
                    print("⚠️ Descarga de PREI incompleta con archivos pendientes")
            elif choice == "3":
                print("📄 Cargando facturas...")
                exito_facturas = self.facturas_manager.cargar_facturas()
                if exito_facturas:
                    print("✅ Carga de facturas completada")
                else:
                    print("⚠️ Carga de facturas pendientes")
            elif choice == "4":
                print("🔄 Integrando información...")

                self.data_integration.integrar_datos(PREI_processed_path, ALTAS_processed_path, FACTURAS_processed_path)
                
                print("✅ Integración completada")
            elif choice == 'auto':
                exito_descarga_altas = self.sai_manager.descargar_altas(temporal_altas_path)
                
                if exito_descarga_altas:
                    exito_descarga_prei = self.prei_manager.descargar_PREI(temporal_prei_path)
                    self.downloaded_files_manager.manage_downloaded_files(temporal_altas_path)
                    print("✅ Descarga de Altas completada")
                    if exito_descarga_prei:
                        print("✅ Descarga de PREI completada")
                        self.downloaded_files_manager.manage_downloaded_files(temporal_prei_path)
                        exito_facturas = self.facturas_manager.cargar_facturas()
                        if exito_facturas:
                            print("✅ Carga de facturas completada")
                            self.data_integration.integrar_datos(PREI_processed_path, ALTAS_processed_path, FACTURAS_processed_path)
                            print("✅ Integración completada")
                        else:
                            print("⚠️ Carga de facturas pendientes")

            elif choice == "5":
                print("🔄 Actualizando información en SQL...")
                
                # Use get_newest_file method to find the integration file
                integration_file, date_integration_file= self.data_integration.get_newest_file(self.integration_path)
                print(f"📁 Using integration file: {os.path.basename(integration_file)} del día {date_integration_file}")
                if integration_file is None:
                    print("❌ No integration file found")
                    continue
                
                print(f"📁 Using integration file: {os.path.basename(integration_file)}")
                
                try:
                    df_to_upload = pd.read_excel(integration_file, sheet_name='df_altas')
                    df_to_upload[['fechaAltaTrunc', 'fpp']] = df_to_upload[['fechaAltaTrunc', 'fpp']].apply(pd.to_datetime, errors='coerce', format='%d/%m/%Y')
                    df_to_upload = self.sql_integration.sql_column_correction(df_to_upload)
                    schema = 'eseotres'
                    table_name = 'df_altas'               
                    self.sql_integration.update_sql(df_to_upload, schema, table_name)
                    
                    print("✅ Actualización completada")
                except Exception as e:
                    print(f"❌ Error during SQL update: {e}")

                print("✅ Actualización completada")
            elif choice == "6":
                print("Generación de agrupaciones y reportes")
                schema = 'eseotres'
                table_name = 'df_altas'                
                queries_folder = os.path.join(self.folder_root, "sql_queries")
                # Ensure the queries folder exists
                if not os.path.exists(queries_folder):
                    print(f"⚠️ Queries folder not found: {queries_folder}")
                else:
                    self.sql_integration.run_queries(queries_folder, schema, table_name)
            elif choice == "0":
                print("Saliendo de la aplicación...")
                break
if __name__ == "__main__":
    app = MiniImssApp()
    app.run()

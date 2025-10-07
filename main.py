import os
from config import ConfigManager
from web_automation_driver import WebAutomationDriver
from SAI import SAI_MANAGEMENT
from PREI import PREI_MANAGEMENT
from facturas_imss import FACTURAS_IMSS
from downloaded_files_manager import DownloadedFilesManager
from data_integration import DataIntegration
from sql_connexion_updating import SQL_CONNEXION_UPDATING
import pandas as pd
import glob 
from data_warehouse import DataWarehouse
from Scripts.Reporting import sql_to_latex


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
        self.sql_integration = SQL_CONNEXION_UPDATING(self.integration_path, self.data_access)
        self.data_warehouse = DataWarehouse(self.data_access, self.working_folder)
        self.queries_folder = os.path.join(self.folder_root, "sql_queries")
        self.sql_to_latex = sql_to_latex.SQL_TO_LATEX(self.working_folder, self.data_access, self.queries_folder)
        print("✅ Inicialización completada")
        return True
        
    def run(self):
        """Ejecuta el menú principal de la aplicación"""
        if not self.initialize():
            return
        altas_path = os.path.join(self.working_folder, "SAI")
        temporal_altas_path = os.path.join(altas_path, "Temporal downloads")
        os.makedirs(temporal_altas_path, exist_ok=True)
        PREI_path = os.path.join(self.working_folder, "PREI")
        temporal_prei_path = os.path.join(PREI_path, 'Temporal downloads')
        os.makedirs(temporal_prei_path, exist_ok=True)

        while True:
            print("\n" + "="*50)
            choice = input(
                "Elige una opción:\n"
                "Extracción:\n"
                "\t1) Descargar altas\n"
                "\t2) Descargar PREI\n"
                "\t3) Cargar facturas\n"
                "Transformación:\n"
                "\t4) Integrar información\n"
                "Carga:\n"
                "\t5) Actualizar SQL (Longitudinal)\n"                
                "\t6) Ejecutar consultas SQL\n"
                "Análisis:\n"
                "\t7) Inteligencia de negocios\n"
                "\t8) Reportes latex\n"
                "\t0) Salir\n"
                "\tauto) Ejecutar 1-6 automáticamente\n"
            ).strip()
        
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
                self.data_integration.integrar_datos()

            elif choice == "5":
                print("🔄 Actualizando SQL (Longitudinal)")
                self.sql_integration.postgresql_main_menu()
                print("Generación de agrupaciones y reportes")

            elif choice == "6":
                print("Ejecutando consultas SQL...")
                # Ensure the queries folder exists
                if not os.path.exists(self.queries_folder):
                    print(f"⚠️ Queries folder not found: {self.queries_folder}")
                else:
                    self.sql_integration.run_queries(self.queries_folder)
                
            elif choice == "7":
                print("Inteligencia de negocios.")
                self.data_warehouse.Business_Intelligence()

            elif choice == "8":
                print("Reportes LaTeX.")
                self.sql_to_latex.reporting_latex_run()

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
                        self.data_integration.integrar_datos()
                        print("✅ Integración completada")
                        print("✅ Cargando a SQL")
                        self.sql_integration.postgresql_main_menu()
                        print("✅ Corriendo Queries")                        
                        self.sql_integration.run_queries(self.queries_folder)
                    else:
                        print("⚠️ No pudimos continuar con el proceso ETL en automático")
            elif choice == "0":
                print("Saliendo de la aplicación...")
                break


if __name__ == "__main__":
    app = MiniImssApp()
    app.run()

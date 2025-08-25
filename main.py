import os
from config import ConfigManager
from helpers import message_print, create_directory_if_not_exists
from web_automation_driver import WebAutomationDriver
from SAI import SAI_MANAGEMENT
from PREI import PREI_MANAGEMENT
from facturas_imss import FACTURAS_IMSS
from downloaded_files_manager import DownloadedFilesManager

class MiniImssApp:
    def __init__(self):
        self.folder_root = os.getcwd()
        self.working_folder = os.path.join(self.folder_root, "Implementación")
        self.config_manager = ConfigManager(self.working_folder)
        self.web_driver = None
        self.data_access = None 

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
        print("✅ Inicialización completada")
        return True

    def run(self):
        """Ejecuta el menú principal de la aplicación"""
        if not self.initialize():
            return
        
        while True:
            print("\n" + "="*50)
            choice = input(message_print(
                "Elige una opción:\n"
                "\t1) Descargar altas\n"
                "\t2) Descargar PREI\n"
                "\t3) Cargar facturas\n"
                "\t4) Integrar información\n"
                "\t0) Salir"
            )).strip()
        
            if choice == "1":
                altas_path = os.path.join(self.working_folder, "SAI")
                temporal_altas_path = os.path.join(altas_path, "Temporal downloads")
                create_directory_if_not_exists(temporal_altas_path)
                self.downloaded_files_manager.manage_downloaded_files(temporal_altas_path)
                exito_descarga_altas = self.sai_manager.descargar_altas(temporal_altas_path)
                if exito_descarga_altas:
                    print("✅ Descarga de Altas completada")
                    self.downloaded_files_manager.manage_downloaded_files(temporal_altas_path)
                else:
                    print("❌ Error en descarga de Altas")
            elif choice == "2":
                PREI_path = os.path.join(self.working_folder, "PREI")
                temporal_prei_path = os.path.join(PREI_path, 'Temporal downloads')
                create_directory_if_not_exists(temporal_prei_path)
                self.downloaded_files_manager.manage_downloaded_files(temporal_prei_path)
                exito_descarga_prei = self.prei_manager.descargar_PREI(temporal_prei_path)
                if exito_descarga_prei:
                    print("✅ Descarga de PREI completada")
                    self.downloaded_files_manager.manage_downloaded_files(temporal_prei_path)
                else:
                    print("⚠️ Descarga de PREI completada con archivos pendientes")
            elif choice == "3":
                print("📄 Cargando facturas...")
                self.facturas_manager.cargar_facturas()
                print("✅ Carga de facturas completada")
            elif choice == "4":
                print("🔄 Integrando información...")
                # Aquí implementarás la lógica de integración
                print("✅ Integración completada")
            elif choice == "0":
                print("👋 ¡Hasta luego!")
                break                
            else:
                print("\n⚠️ Elige una opción válida (0-4). Inténtalo de nuevo.\n")

if __name__ == "__main__":
    app = MiniImssApp()
    app.run()
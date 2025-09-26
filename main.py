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
import glob 
from data_warehouse import DataWarehouse

class MiniImssApp:
    def __init__(self):
        self.folder_root = os.getcwd()
        self.working_folder = os.path.join(self.folder_root, "Implementación")
        self.config_manager = ConfigManager(self.working_folder)
        self.web_driver = None
        self.data_access = None 
        self.integration_path = os.path.join(self.working_folder, "Integración")
        
    def update_sql_historico(self):
        print("🔄 Integrando información...")
        print("Fuente de las altas históricas")
        df_final, esquema, tabla = self.altas_historicas()
        print(df_final.head(2))
        try:
            df_final[['fechaAltaTrunc', 'fpp']] = df_final[['fechaAltaTrunc', 'fpp']].apply(pd.to_datetime, errors='coerce', format='%d/%m/%Y')
            df_final = self.sql_integration.sql_column_correction(df_final)         
            self.sql_integration.update_sql(df_final, esquema, tabla)
            # Cambio a diccionario
            print(f"✅ Actualización {esquema}.{tabla} completada")
        except Exception as e:
            print(f"❌ Error durante la actualización: {e}")
        
        print("✅ Integración completada")    
          
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
        self.data_warehouse = DataWarehouse(self.data_access, self.working_folder)
        print("✅ Inicialización completada")
        return True
    def altas_historicas(self):
        print("🔄 Actualizando información en SQL: longitudinal en el tiempo")

        # Buscar archivos .xlsx en la carpeta de integración

        integration_files = glob.glob(os.path.join(self.integration_path, "*.xlsx"))
        schema = 'eseotres_warehouse'
        table_name = 'altas_historicas'       
        

        # Columnas esperadas: base + integración (sin duplicados, preservando orden)
        base_cols = list(self.data_access['columns_IMSS_altas'])
        columnas_integracion = ['file_date', 'UUID', 'Estado C.R.']
        columnas = list(dict.fromkeys(base_cols + columnas_integracion))

        # Debug
        print(f"🔍 Carpeta de integración: {self.integration_path}")
        print(f"🗂️ Archivos encontrados: {len(integration_files)}")
        print(f"🧩 Columnas esperadas ({len(columnas)}): {columnas}")

        # Filtrar: aceptar archivos que contengan al menos todas las columnas esperadas
        valid_files = []
        for path in integration_files:
            try:
                cols = list(pd.read_excel(path, nrows=0).columns)
                if set(columnas).issubset(set(cols)):
                    valid_files.append(path)
                else:
                    missing = [c for c in columnas if c not in cols]
                    extra = [c for c in cols if c not in columnas]
                    print(f"⚠️ {os.path.basename(path)} faltan: {missing} | extras: {extra}")
            except Exception as e:
                print(f"⚠️ No se pudo leer {os.path.basename(path)}: {e}")

        if not valid_files:
            print("❌ No hay archivos válidos con columnas esperadas")
            return pd.DataFrame(columns=columnas)

        # Cargar cada Excel, quedarnos solo con las columnas esperadas y concatenar
        partes = []
        for p in valid_files:
            try:
                df = pd.read_excel(p)
                df = df.loc[:, columnas]  # solo esperadas, en el orden definido
                partes.append(df)
            except Exception as e:
                print(f"⚠️ Error leyendo {os.path.basename(p)}: {e}")

        if not partes:
            print("❌ No se pudo cargar ningún archivo válido")
            return pd.DataFrame(columns=columnas)

        df_final = pd.concat(partes, ignore_index=True)
        print(f"✅ {len(valid_files)} archivos válidos concatenados: {len(df_final)} filas")
        return df_final, schema, table_name

        
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
        queries_folder = os.path.join(self.folder_root, "sql_queries")
        while True:
            print("\n" + "="*50)
            choice = input(message_print(
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
                "\t0) Salir"
                "ETL automático: "
                "\tauto Ejecutar todo automáticamente\n"
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
                self.data_integration.integrar_datos()

            elif choice == "5":
                print("🔄 Actualizando SQL (Longitudinal)")
                self.update_sql_historico()
                print("Generación de agrupaciones y reportes")


            elif choice == "6":
                print("Ejecutando consultas SQL...")
                # Ensure the queries folder exists
                if not os.path.exists(queries_folder):
                    print(f"⚠️ Queries folder not found: {queries_folder}")
                else:
                    self.sql_integration.run_queries(queries_folder)
                
            elif choice == "7":
                print("Inteligencia de negocios.")
                self.data_warehouse.Business_Intelligence()

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
                        self.update_sql_historico()
                        self.sql_integration.run_queries(queries_folder)
                    else:
                        print("⚠️ No pudimos continuar con el proceso ETL en automático")
            elif choice == "0":
                print("Saliendo de la aplicación...")
                break


if __name__ == "__main__":
    app = MiniImssApp()
    app.run()

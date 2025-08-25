import pandas as pd
import datetime 
import os
import glob
from helpers import message_print, create_directory_if_not_exists


class DataIntegration:
    def __init__(self, working_folder, data_access):
        self.working_folder = working_folder
        self.data_access = data_access  # ✅ Agregué esta línea que faltaba
        
    def integrar_datos(self, prei_path, altas_path, facturas_path):
        print(f"🔍 Buscando archivos más recientes...")
        
        newest_altas_file, altas_date = self.get_newest_file(altas_path, "*.xlsx")
        newest_prei_file, prei_date = self.get_newest_file(prei_path, "*.xlsx")
        newest_facturas_file, facturas_date = self.get_newest_file(facturas_path, "*.xlsx")
        
        print(f"📊 Archivos encontrados:")
        print(f"   Altas: {newest_altas_file} ({altas_date})")
        print(f"   PREI: {newest_prei_file} ({prei_date})")
        print(f"   Facturas: {newest_facturas_file} ({facturas_date})")

        # Cargar DataFrames
        df_altas = pd.read_excel(newest_altas_file) if newest_altas_file else pd.DataFrame()
        df_prei = pd.read_excel(newest_prei_file) if newest_prei_file else pd.DataFrame()
        df_facturas = pd.read_excel(newest_facturas_file) if newest_facturas_file else pd.DataFrame()

        # Agregar fechas de archivo a cada DataFrame
        if not df_altas.empty and altas_date:
            df_altas['file_date'] = altas_date
        if not df_prei.empty and prei_date:
            df_prei['file_date'] = prei_date  
        if not df_facturas.empty and facturas_date:
            df_facturas['file_date'] = facturas_date

        # Crear carpeta de integración
        from helpers import create_directory_if_not_exists
        integration_path = os.path.join(self.working_folder, "Integración")
        create_directory_if_not_exists(integration_path)

        # Encontrar la fecha más antigua
        fechas_disponibles = []
        if altas_date:
            fechas_disponibles.append(altas_date)
        if prei_date:
            fechas_disponibles.append(prei_date)
        if facturas_date:
            fechas_disponibles.append(facturas_date)

        if fechas_disponibles:
            oldest_date = min(fechas_disponibles)
            oldest_date_str = oldest_date.strftime("%Y-%m-%d")
            
            # Crear nombre del archivo
            xlsx_path = os.path.join(integration_path, f"{oldest_date_str} Integracion.xlsx")
            
            try:
                # Guardar múltiples hojas en un archivo Excel
                with pd.ExcelWriter(xlsx_path, engine='openpyxl') as writer:
                    if not df_altas.empty:
                        df_altas.to_excel(writer, sheet_name='df_altas', index=False)
                        print(f"✅ Hoja 'df_altas' guardada con {len(df_altas)} filas")
                    
                    if not df_prei.empty:
                        df_prei.to_excel(writer, sheet_name='df_prei', index=False)
                        print(f"✅ Hoja 'df_prei' guardada con {len(df_prei)} filas")
                    
                    if not df_facturas.empty:
                        df_facturas.to_excel(writer, sheet_name='df_facturas', index=False)
                        print(f"✅ Hoja 'df_facturas' guardada con {len(df_facturas)} filas")
                
                print(f"\n🎉 ¡Integración completada exitosamente!")
                print(f"📁 Archivo guardado en: {xlsx_path}")
                print(f"📅 Fecha de integración basada en: {oldest_date_str}")
                
            except Exception as e:
                print(f"❌ Error al guardar archivo de integración: {e}")
        else:
            print("⚠️ No se encontraron archivos válidos para integrar")



    def get_newest_file(self, path, pattern="*.xlsx"): 
        """
        Obtiene el archivo más reciente basado en la fecha en el nombre del archivo.
        Formato esperado: YYYY-MM-DD_HHMM-...
        """
        date_format = "%Y-%m-%d_%H%M"
        today = datetime.date.today()
        
        if not os.path.exists(path):
            print(f"⚠️ La carpeta {path} no existe")
            return None, None
        
        # Obtener todos los archivos que coincidan con el patrón
        files = glob.glob(os.path.join(path, pattern))
        
        if not files:
            print(f"⚠️ No se encontraron archivos {pattern} en {os.path.basename(path)}")
            return None, None
        
        newest_file = None
        newest_date = None
        
        for file_path in files:
            filename = os.path.basename(file_path)
            
            try:
                # Extraer la fecha del inicio del nombre del archivo
                # Formato esperado: 2025-08-25_1317-SAI Altas.xlsx
                date_part = filename.split('-', 3)  # Divide en máximo 4 partes
                if len(date_part) >= 3:
                    # Reconstruir fecha: YYYY-MM-DD_HHMM
                    date_str = f"{date_part[0]}-{date_part[1]}-{date_part[2]}"
                    
                    # Extraer hora si existe (después del tercer guión)
                    if '_' in date_str:
                        date_str = date_str  # Ya tiene el formato correcto
                    else:
                        date_str += "_0000"  # Agregar hora por defecto
                    
                    file_date = datetime.datetime.strptime(date_str, date_format)
                    
                    if newest_date is None or file_date > newest_date:
                        newest_date = file_date
                        newest_file = file_path
                        
            except (ValueError, IndexError) as e:
                print(f"⚠️ No se pudo extraer fecha de {filename}: {e}")
                continue
        
        if newest_file:
            file_date_only = newest_date.date()
            
            # Verificar si el archivo es de hoy
            if file_date_only < today:
                print(f"⚠️ El archivo {os.path.basename(newest_file)} no es de hoy ({file_date_only}), se recomienda descargar")
            
            return newest_file, newest_date
        else:
            print(f"❌ No se pudo determinar el archivo más reciente en {os.path.basename(path)}")
            return None, None
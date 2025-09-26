import os
import time
import datetime
import pandas as pd
from lxml import etree
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.keys import Keys
from selenium.common.exceptions import TimeoutException
from helpers import create_directory_if_not_exists, message_print
import numpy as np

class FACTURAS_IMSS:
    def __init__(self, working_folder, data_access):
        self.working_folder = working_folder
        self.data_access = data_access
        
    def cargar_facturas(self):
        facturas_folder = os.path.join(self.working_folder, "Facturas")
        xlsx_database = os.path.join(facturas_folder, 'xmls_extraidos.xlsx')
        self.smart_xml_extraction(xlsx_database)   
        
        preffix = os.path.basename(facturas_folder)
        create_directory_if_not_exists(facturas_folder)
        
        
        # ✅ Usar carpeta local en lugar de Dropbox
        consultas_folder = os.path.join(facturas_folder, "Consultas")
        create_directory_if_not_exists(consultas_folder)

        # DataFrame general vacío
        df_general = pd.DataFrame()

        # Iterar sobre los paquetes en PAQS_IMSS
        for paq_name, paq_info in self.data_access.get("PAQS_IMSS", {}).items():
            file_path = paq_info.get("file_path")
            sheet = paq_info.get("sheet")
            rows = paq_info.get("rows", [])

            print(f"\n🔍 Procesando {paq_name}")

            # 1. Intentar cargar archivo
            if not os.path.exists(file_path):
                print(f"⚠️ Archivo no encontrado: {file_path}")
                continue
            print(f"✅ Archivo encontrado: {file_path}")

            try:
                # 2. Intentar cargar hoja con columnas definidas
                df = pd.read_excel(file_path, sheet_name=sheet, usecols=rows)
                print(f"✅ Hoja '{sheet}' cargada con columnas {rows}")

                # Concatenar a df_general
                df_general = pd.concat([df_general, df], ignore_index=True)

            except ValueError as e:
                print(f"⚠️ Problema con la hoja o columnas: {e}")
                continue

        # Guardar resultado en carpeta local
        if not df_general.empty:
            today = datetime.datetime.today().strftime("%Y-%m-%d-%H")  # ✅ Formato de fecha corregido
            output_file = os.path.join(consultas_folder, f"{today}_PAQ_IMSS.xlsx")  # ✅ Usar carpeta local
            df_xmls = pd.read_excel(xlsx_database)
            print(f"📊 Filas en df_xmls antes de limpiar: {df_xmls.shape[0]}")

            # Verificar duplicados por Folio
            duplicados = df_xmls['Folio'].duplicated().sum()
            if duplicados > 0:
                print(f"⚠️ Se encontraron {duplicados} folios duplicados en df_xmls")
                
                # Opción A: Eliminar duplicados manteniendo el primero
                df_xmls = df_xmls.drop_duplicates(subset=['Folio'], keep='first')
                print(f"✅ Duplicados eliminados. Filas restantes: {df_xmls.shape[0]}")
            print(f"Filas antes de la fusión con el XML {df_general.shape[0]}")
            df_general = pd.merge(df_general, df_xmls, how='left', left_on='Folio', right_on='Folio')
            print(f"print filas después de la fusión con el XML {df_general.shape[0]}")
            print("\n✅ DataFrame fusionado con información XL con éxito.")
            try:
                df_general.to_excel(output_file, index=False)
                print(f"\n💾 Archivo guardado en {output_file}")
                print(f"📊 Total de filas procesadas: {len(df_general)}")
                return True
            except PermissionError as e:
                print(f"❌ Error de permisos: {e}")
                print(f"🔄 Intentando guardar en carpeta alternativa...")
                
                # Fallback: guardar en carpeta temporal
                import tempfile
                temp_dir = tempfile.gettempdir()
                fallback_file = os.path.join(temp_dir, f"{today}_facturas.xlsx")
                df_general.to_excel(fallback_file, index=False)
                print(f"💾 Archivo guardado en ubicación temporal: {fallback_file}")

                return False
                
        else:
            print("\n⚠️ No se generó DataFrame, revisar configuración.")        

        

        #self.validacion_de_paqs(dict_path_sheet, dic_columnas, facturas_folder, altas_path, altas_sheet, info_types, xlsx_database)


    def smart_xml_extraction(self, xlsx_database):
        # Si la base existe, la cargamos; si no, creamos el DataFrame con todas las columnas, incluida la nueva 'Fecha'
        print(message_print("Extrayendo la información de los XMLs..."))
        invoice_paths = []
        for path in self.data_access['facturas_path']:
            if os.path.exists(path):
                invoice_paths.append(path)

        if os.path.exists(xlsx_database):
            df_database = pd.read_excel(xlsx_database)
        else:
            df_database = pd.DataFrame(columns=[
                'UUID', 'Folio', 'Fecha', 'Nombre', 'Rfc',
                'Descripcion', 'Cantidad', 'Importe', 'Archivo'
            ])

        data = []

        for folder in invoice_paths:
            print(f"\nExplorando carpeta: {folder}")
            
            for root_dir, dirs, files in os.walk(folder):
                for file in files:
                    if not file.endswith('.xml'):
                        continue
                    full_path = os.path.join(root_dir, file)

                    try:
                        tree = etree.parse(full_path)
                        root_element = tree.getroot()

                        # Detectar namespace CFDI y TimbreFiscalDigital
                        ns = None
                        for ns_url in root_element.nsmap.values():
                            if "cfd/3" in ns_url:
                                ns = {
                                    "cfdi": "http://www.sat.gob.mx/cfd/3",
                                    "tfd": "http://www.sat.gob.mx/TimbreFiscalDigital"
                                }
                                break
                            elif "cfd/4" in ns_url:
                                ns = {
                                    "cfdi": "http://www.sat.gob.mx/cfd/4",
                                    "tfd": "http://www.sat.gob.mx/TimbreFiscalDigital"
                                }
                                break
                        if ns is None:
                            continue

                        # Extraer Folio y Serie, y construir Folio completo
                        folio = root_element.get('Folio')
                        serie = root_element.get('Serie')
                        folio_completo = f"{serie}-{folio}"

                        # Extraer Fecha del <cfdi:Comprobante>
                        fecha = root_element.get('Fecha')

                        # Extraer UUID desde <tfd:TimbreFiscalDigital>
                        uuid = None
                        complemento = root_element.find('./cfdi:Complemento', ns)
                        if complemento is not None:
                            timbre = complemento.find('./tfd:TimbreFiscalDigital', ns)
                            if timbre is not None:
                                uuid = timbre.get('UUID')

                        # Saltar si ya existe (por UUID o por Folio+Archivo)
                        if uuid:
                            if (df_database['UUID'] == uuid).any():
                                continue
                        else:
                            if ((df_database['Folio'] == folio_completo) & 
                                (df_database['Archivo'] == file)).any():
                                continue

                        # Extraer receptor
                        rec = root_element.find('./cfdi:Receptor', ns)
                        if rec is None:
                            continue
                        nombre = rec.get('Nombre')
                        rfc = rec.get('Rfc')

                        # Extraer cada concepto
                        for concepto in root_element.findall('./cfdi:Conceptos/cfdi:Concepto', ns):
                            descripcion = concepto.get('Descripcion')
                            cantidad    = concepto.get('Cantidad')
                            importe     = concepto.get('Importe')

                            data.append([
                                uuid,
                                folio_completo,
                                fecha,
                                nombre,
                                rfc,
                                descripcion,
                                cantidad,
                                importe,
                                file
                            ])

                    except Exception as e:
                        print(f"[ERROR] Al procesar {file}: {e}")

        # Si hay nuevos registros, los agregamos y salvamos
        if data:
            df_nuevos = pd.DataFrame(data, columns=[
                'UUID', 'Folio', 'Fecha', 'Nombre', 'Rfc',
                'Descripcion', 'Cantidad', 'Importe', 'Archivo'
            ])
            df_database = pd.concat([df_database, df_nuevos], ignore_index=True)
            df_database[['Cantidad', 'Importe']] = df_database[['Cantidad', 'Importe']].astype(float)
            df_database.to_excel(xlsx_database, engine='openpyxl', index=False)
            print(f"\n✅ Se agregaron {len(df_nuevos)} nuevos registros a {xlsx_database}")
        else:
            print("\n✔️ No se encontraron nuevos XMLs para agregar.")


    # ==== 
    # SECCIÓN PARA CARGAR LOS ARCHIVOS DE PAQ
    # ====
    def validacion_de_paqs(self, dict_path_sheet, dic_columnas, paq_folder, altas_path, altas_sheet, info_types, xlsx_database):
        # (I) Carga
        df_entregas_o_altas = pd.read_excel(altas_path, sheet_name=altas_sheet)
        columnas_objetivo = ["Folio", "Referencia", "Alta", "Total", "UUID"]
        df_facturas = pd.DataFrame(columns=columnas_objetivo)

        # (2) Iteramos simultáneamente sobre ambos diccionarios. 
        #     Se asume que dict_path_sheet y dic_columnas ya están "alineados" en el mismo orden de inserción.
        for (ruta_excel, nombre_hoja), lista_cols in zip(dict_path_sheet.items(), dic_columnas.values()):
            # ruta_excel: p.ej. r"C:\Users\arman\Dropbox\FACT 2023\Generacion facturas IMSS VFinal.xlsx"
            # nombre_hoja:    p.ej. "Reporte Paq"
            # lista_cols:     p.ej. ["Folio", "Referencia", "Alta", "Total", "UUID"]

            # (3) Leemos únicamente las columnas indicadas en lista_cols
            df_temp = pd.read_excel(
                ruta_excel,
                sheet_name=nombre_hoja,
                usecols=lista_cols
            )

            # (4) Concatenamos al DataFrame global
            df_facturas = pd.concat([df_facturas, df_temp], ignore_index=True)
        # (II) Limpia

        # (II.1) Corrección de tipos, remover duplicados y lógica de referencias.
        df_entregas_o_altas, df_facturas = self.correccion_types(df_entregas_o_altas, df_facturas, info_types)

        print("Información del dataframe altas: \n")
        print(df_entregas_o_altas.info())
        print("Información del dataframe de facturas: \n")
        print(df_facturas.info())
        excel_facturas= os.path.join(paq_folder, f"{info_types}.xlsx")

        # III.I Cargar IMSS y ligar.     
        df_altas_df_facturas = {
            'noAlta': 'Alta',
            'noOrden': 'Referencia'
        }

        df_entregas_o_altas['Factura'] = self.multi_column_lookup(
            df_to_fill=df_entregas_o_altas,
            df_to_consult=df_facturas,
            match_columns=df_altas_df_facturas,
            return_column='Folio',
            default_value='sin match'
        )
        # III.II Cargar IMSS y ligar.     
        df_altas_df_facturas = {
            'Alta': 'noAlta',
            'Referencia': 'noOrden'
        }

        df_facturas['Alta_ligada'] = self.multi_column_lookup(
            df_to_fill=df_facturas,
            df_to_consult=df_entregas_o_altas,
            match_columns=df_altas_df_facturas,
            return_column='noAlta',
            default_value='alta no localizada'
        )
        

        df_facturas.to_excel(excel_facturas, index=False)

        #IV Sobreescribir UUID y totales 
        print("Vamos a poblar el UUID de la base de facturación con info extraída de los XML's")
        if os.path.exists(xlsx_database):
            columna_UUID ='UUID'
            df_database = pd.read_excel(xlsx_database)
            df_database = (
                df_database
                .drop_duplicates(subset='UUID', keep='first')
                .reset_index(drop=True)
            )
            df_UUIDS = {'Folio': 'Folio'}
            df_facturas['UUID'] = self.multi_column_lookup(
                df_to_fill=df_facturas,
                df_to_consult=df_database,
                match_columns=df_UUIDS,
                return_column=columna_UUID,
                default_value=f'{columna_UUID} no localizado'
            )
        
        if os.path.exists(xlsx_database):
            columna_retorno ='Importe'
            columna_poblar = 'Total'
            print(f"Vamos a poblar l columna {columna_poblar} con de la columna {columna_retorno} base de facturación con info extraída de los XML's")
            df_database = pd.read_excel(xlsx_database)
            df_database = (
                df_database
                .drop_duplicates(subset='UUID', keep='first')
                .reset_index(drop=True)
            )
            columns_totales_match = {'Folio': 'Folio'}
            df_facturas[columna_poblar] = self.multi_column_lookup(
                df_to_fill=df_facturas,
                df_to_consult=df_database,
                match_columns=columns_totales_match,
                return_column=columna_retorno,
                default_value=f'{columna_UUID} no localizado'
            )
            
        # Cargar el archivo conservando las hojas
        with pd.ExcelWriter(altas_path, engine='openpyxl', mode='a', if_sheet_exists='replace') as writer:
            df_entregas_o_altas.to_excel(writer, sheet_name=altas_sheet, index=False)

        print("\nExcel generado de facturas generado exitosamente\n")

    def multi_column_lookup(self, df_to_fill, df_to_consult, match_columns: dict, return_column, default_value='sin match'):
        """
        Realiza búsqueda con múltiples columnas y retorna valor o advertencias
        Args:
            df_to_fill (pd.DataFrame): DataFrame que queremos llenar.
            df_to_consult (pd.DataFrame): DataFrame que consultamos.
            match_columns (dict): {col_df_to_fill: col_df_to_consult} pares de columnas para hacer match.
            return_column (str): Columna en df_to_consult con el valor a traer.
            default_value (any): Valor si no hay coincidencia.
        Returns:
            pd.Series: Valores para poblar la columna deseada.
        """

        if not isinstance(df_to_consult, pd.DataFrame):
            raise TypeError(f"El argumento 'df_to_consult' debe ser un DataFrame, se recibió: {type(df_to_consult)}")

        result_list = []
        message_falta_liga = 'Renglones del dataframe a llenar sin filtro en el dataframe a consultar'
        ligas_duplicadas = 0
        ligas_vacías = 0
        for _, row in df_to_fill.iterrows():
            # Construir filtro booleano dinámico
            mask = pd.Series([True] * len(df_to_consult))

            for source_col, consult_col in match_columns.items():
                mask &= df_to_consult[consult_col] == row[source_col]

            filtered = df_to_consult[mask]

            if filtered.empty:
                result_list.append(default_value)
                print("\tEncontramos renglones sin poder ligar")
                ligas_vacías += 1
            elif len(filtered) > 1:
                resultados_duplicados = ", ".join(filtered[return_column].astype(str).tolist())
                result_list.append(f'Peligro: 2 resultados {resultados_duplicados}')
                ligas_duplicadas += 1
            else:
                result_list.append(filtered.iloc[0][return_column])
        success_message = "✅ Se ligaron el 100% de los renglones y no hubo duplicados."
        if ligas_duplicadas == 0 and ligas_vacías == 0:
            print(f"{'*'*len(success_message)}\n{success_message}\n{'*'*len(success_message)}")
        elif ligas_duplicadas == 0 and ligas_vacías > 0:
            print("⚠️ Hay renglones que no se pudieron llenar con el dataframe consultado.")
        elif ligas_duplicadas > 0 and ligas_vacías == 0:
            print("⚠️ Hay renglones para los que se encontraron más de un resultado en el dataframe de consulta.")
        else:
            print("⚠️ Hay renglones vacíos y renglones con duplicados.")

        return pd.Series(result_list, index=df_to_fill.index)

    def correccion_types(self, df_entregas_o_altas, df_facturas, info_types):
        if info_types == 'IMSS': 
            print(f"Iniciamos la corrección de tipos para el {info_types}")
            print(f"Número de filas del dataframe facturas al iniciar {df_facturas.index.size}")
            print(f"Número de filas del dataframe altas al iniciar {df_entregas_o_altas.index.size}")
            # El siguiente paso es debido a la existencia de valores infinitos en la columna Referencia
            df_facturas['Referencia'] = (
                df_facturas['Referencia']
                .replace([np.inf, -np.inf], np.nan)  # Inf  → NaN
                .fillna(0)                           # NaN  → 0
                .astype('int64')                     # ahora ya solo hay valores válidos para int64
            )        

            df_facturas['Referencia'] = df_facturas['Referencia'].astype('int64')
            df_entregas_o_altas['noOrden'] = df_entregas_o_altas['noOrden'].astype('int64')
        
            # (II.2) Duplicados. 
            duplicados_facturas = df_facturas.duplicated().sum()
            print(f"El dataframe facturas tiene {duplicados_facturas} filas duplicadas, vamos a removerlas\n")
            df_facturas = df_facturas.drop_duplicates()
            # (II.3) Ausentes
            print("Removemos del dataframe facturas aquellas filas con Alta y Orden vacíos\n")
            mask = ((df_facturas['Referencia'].isna() | (df_facturas['Referencia'] == 0)) & df_facturas['Alta'].isna())
            print(f"Totales de las filas con Referencia y Alta ausentes = {mask.index.size}")
            df_facturas = df_facturas.loc[~mask].reset_index(drop=True)
            
            df_facturas = (
                df_facturas[~df_facturas['Total'].isin([0, '', ' '])]
                .dropna(subset=['Total'])
                .reset_index(drop=True)
            )

            return df_entregas_o_altas, df_facturas

        else: 
            print("no considerado aún")
import pandas as pd
import datetime 
import os
import glob

from sympy import group
from helpers import message_print, create_directory_if_not_exists
import yaml
import re
from collections import defaultdict
import numpy as np
import datetime
import json


class DataIntegration:
    def __init__(self, working_folder, data_access, integration_path):
        self.working_folder = working_folder
        self.data_access = data_access  
        self.integration_path = integration_path 
        
        self.prei_path=os.path.join(self.working_folder, "PREI", "PREI_files")
        self.altas_path=os.path.join(self.working_folder, "SAI", "SAI Altas_files")
        self.facturas_path=os.path.join(self.working_folder, "Facturas", "Consultas")
        self.ordenes_path=os.path.join(self.working_folder, "SAI", "SAI Orders_files")
        self.folders = {
            "prei": self.prei_path,
            "altas": self.altas_path,
            "facturas": self.facturas_path,
            "ordenes": self.ordenes_path
            }
        self.record_file=os.path.join(self.working_folder,"processed_file.db")

    def generate_file_groups(self):
        print(self.folders)
        from datetime import datetime, timedelta
        print(f"🔍 Buscando archivos más recientes...")
        # Regex para extraer yyyy-mm-dd-hh
        ts_pattern = re.compile(r"^(\d{4}-\d{2}-\d{2}-\d{2})")
        
        # 1. Escanear todos los archivos
        all_files = []
        for cat, folder in self.folders.items():
            if not os.path.exists(folder):
                continue
            for f in os.listdir(folder):
                if f.endswith(".xlsx"):
                    m = ts_pattern.match(f)
                    if m:
                        ts = datetime.strptime(m.group(1), "%Y-%m-%d-%H")
                        all_files.append((ts, cat, os.path.join(folder, f)))

        # 2. Ordenar por timestamp
        all_files.sort(key=lambda x: x[0])

        # 3. Agrupar con ventana de 2 horas
        groups = []
        current = []
        for ts, cat, path in all_files:
            if not current:
                current.append((ts, cat, path))
            else:
                delta = ts - current[0][0]
                if delta <= timedelta(hours=2):
                    current.append((ts, cat, path))
                else:
                    groups.append(current)
                    current = [(ts, cat, path)]
        if current:
            groups.append(current)

        # 4. Formar all_groups y complete_groups
        all_groups = []
        complete_groups = []
        for g in groups:
            min_ts = min(x[0] for x in g)
            max_ts = max(x[0] for x in g)
            group_id = f"{min_ts.strftime('%Y-%m-%d-%H')}_{max_ts.strftime('%H')}"
            record = {"group_id": group_id}
            for cat in self.folders.keys():
                record[cat] = ""
            for ts, cat, path in g:
                record[cat] = path
            all_groups.append(record)

            if all(record[cat] for cat in self.folders.keys()):
                complete_groups.append(record)

        print("📂 all_groups encontrados:", len(all_groups))
        print("✅ complete_groups completos:", len(complete_groups))
       # 🔎 Analizar si los date-hour coinciden dentro de cada grupo
        exact_match_count = 0
        different_count = 0
        for g in all_groups:
            hours = []
            for cat in self.folders.keys():
                if g[cat]:
                    # Tomamos solo yyyy-mm-dd-hh del path
                    fname = os.path.basename(g[cat])
                    ts_prefix = fname[:13]  # YYYY-MM-DD-HH
                    hours.append(ts_prefix)
            if hours and all(h == hours[0] for h in hours):
                exact_match = True
                exact_match_count += 1
            else:
                exact_match = False
                different_count += 1
            print(f"Grupo {g['group_id']} → same_datehour == {exact_match}")

        print(f"📊 Grupos con misma fecha-hora exacta: {exact_match_count}")
        print(f"📊 Grupos con diferencias de hora: {different_count}")
        complete_groups.sort(
            key=lambda x: x['group_id'][:10],  # yyyy-mm-dd
            reverse=True
        )

        return complete_groups
             
    def IMSS_ordenes_and_altas(self, df_ordenes: pd.DataFrame, df_altas: pd.DataFrame) -> pd.DataFrame:
        """
        Une órdenes con altas IMSS:
        - Join: df_ordenes['orden'] == df_altas['noOrden']
        - Multi-match: duplica la fila de órdenes por cada alta
        - Single-match: una sola fila combinada
        - No-match: columnas de altas en NaN
        Devuelve las columnas en el orden solicitado.
        """

        # --- Columnas necesarias ---
        needed_columns_df_ordenes = [
            'contrato', 'orden', 'cveArticulo', 'fechaExpedicion',
            'descripciónEntrega', 'estatus', 'fechaEntrega',
            'cantidadSolicitada', 'precio', 'importeSinIva', 'file_date'
        ]
        needed_columns_df_altas = ['fechaAltaTrunc', 'noAlta', 'cantRecibida', 'importe']
        join_left = 'orden'
        join_right = 'noOrden'

        # --- Validación de columnas ---
        missing_o = [c for c in needed_columns_df_ordenes if c not in df_ordenes.columns]
        if missing_o:
            raise ValueError(f"df_ordenes le faltan columnas: {missing_o}")

        required_alt_cols = [join_right] + needed_columns_df_altas
        missing_a = [c for c in required_alt_cols if c not in df_altas.columns]
        if missing_a:
            raise ValueError(f"df_altas le faltan columnas: {missing_a}")

        # --- Filtrar a las columnas necesarias ---
        df_o = df_ordenes[needed_columns_df_ordenes].copy()
        df_a = df_altas[required_alt_cols].copy()

        # --- Merge LEFT: respeta 0/1/n matches tal como lo pediste ---
        merged = df_o.merge(
            df_a,
            how='left',
            left_on=join_left,
            right_on=join_right
        )

        # No necesitamos conservar la columna de join derecha
        merged.drop(columns=[join_right], inplace=True)
        # --- Normalize "no delivery" rows right here ---
        merged['cantRecibida'] = merged['cantRecibida'].fillna(0)
        merged['importe'] = merged['importe'].fillna(0)


        # --- Ordenar columnas exactamente como quieres ---
        final_cols = needed_columns_df_ordenes + needed_columns_df_altas
        # Aseguramos que estén todas (si algo faltara por nombres, lanzaríamos)
        for c in final_cols:
            if c not in merged.columns:
                raise RuntimeError(f"Columna esperada no encontrada tras el merge: {c}")

        merged = merged[final_cols]
        # --- After merged = df_o.merge(Rows with partial deliveries) ---

        rows_to_add = []

        for orden_number in merged['orden'].unique():
            subset = merged[merged['orden'] == orden_number]
            total_required = subset['importeSinIva'].iloc[0]
            total_delivered = subset['importe'].fillna(0).sum()
            import_performance = total_required - total_delivered

            if import_performance > 0 and not np.isclose(import_performance, 0):
                # Checar si ya hay fila "vacía" (cantRecibida=0 e importe=0)
                already_has_empty = ((subset['cantRecibida'] == 0) & (subset['importe'] == 0)).any()

                if not already_has_empty:
                    base_row = subset.iloc[0].copy()
                    base_row['fechaAltaTrunc'] = np.nan
                    base_row['noAlta'] = np.nan
                    base_row['cantRecibida'] = 0
                    base_row['importe'] = 0
                    rows_to_add.append(base_row)

                """ 
                if total_delivered == 0:  # 🚩 case: no deliveries at all
                    base_row['cantRecibida'] = 0
                    base_row['importe'] = pieces * precio
                else:  # 🚩 case: partial delivery
                    base_row['cantRecibida'] = 0
                    base_row['importe'] = pieces * precio
                """



        # concatenate the implicit rows to merged
        if rows_to_add:
            merged = pd.concat([merged, pd.DataFrame(rows_to_add)], ignore_index=True)

        # --- Dates parsing ---
        date_columns = ['fechaExpedicion', 'fechaEntrega', 'fechaAltaTrunc']
        for col in date_columns:
            merged[col] = pd.to_datetime(
                merged[col],
                format="%d/%m/%Y",
                errors="coerce"
            )

        today_date = pd.to_datetime(datetime.datetime.now().date())

        # 1. Diferencia normal cuando hay ambas fechas
        merged['days_diff'] = (merged['fechaEntrega'] - merged['fechaAltaTrunc']).dt.days

        # 2. Detectar filas sin fechaAltaTrunc (NaT)
        mask_nan = merged['fechaAltaTrunc'].isna() & merged['fechaEntrega'].notna()

        # 2a. Todavía dentro del plazo (hoy <= fechaEntrega + 5 días)
        mask_still_time = mask_nan & (today_date <= merged['fechaEntrega'] + pd.Timedelta(days=5))
        merged.loc[mask_still_time, 'days_diff'] = (
            (merged.loc[mask_still_time, 'fechaEntrega'] - today_date).dt.days
        )

        # 2b. Plazo perdido (hoy > fechaEntrega + 5 días)
        mask_late = mask_nan & (today_date > merged['fechaEntrega'] + pd.Timedelta(days=5))
        merged.loc[mask_late, 'days_diff'] = -5
        def calcular_cantidad_sancionable(df):
            results = []

            for orden, group in df.groupby("orden", sort=False):
                group = group.sort_values(by="fechaAltaTrunc", na_position="last").copy()

                # Todas las filas empiezan con lo recibido
                group["cantidadSancionable"] = group["cantRecibida"].fillna(0)

                # Calcular faltante (si existe)
                total_recibido = group["cantRecibida"].fillna(0).sum()
                faltante = group["cantidadSolicitada"].iloc[0] - total_recibido

                if faltante > 0:
                    mask_remaining = group["cantRecibida"].fillna(0) == 0
                    if mask_remaining.any():
                        idx_target = mask_remaining.idxmax()  # primera fila con cantRecibida=0
                        group.loc[idx_target, "cantidadSancionable"] = faltante

                results.append(group)

            return pd.concat(results, ignore_index=True)

        # Aplicar sobre tu DataFrame
        merged = calcular_cantidad_sancionable(merged)
        merged['sancion'] = 0.0

        # --- Caso 1: a tiempo o adelantado (days_diff >= 0) ---
        mask_on_time = merged['days_diff'] >= 0
        merged.loc[mask_on_time, 'sancion'] = 0

        # --- Caso 2: atraso (days_diff < 0) ---
        mask_late = merged['days_diff'] < 0

        # Valor absoluto de los días de atraso, capado a 5
        late_days = (-merged.loc[mask_late, 'days_diff']).clip(upper=5)

        # Tasa final = días de atraso * 0.02
        final_rate = late_days * 0.02

        merged.loc[mask_late, 'sancion'] = (
            merged.loc[mask_late, 'cantidadSancionable'] *
            merged.loc[mask_late, 'precio'] *
            final_rate
        ) 

        mask_cancelada = (merged['estatus'] == "Cancelada") & (merged['noAlta'].isna())
        merged.loc[mask_cancelada, 'sancion'] = 0        

        return merged
    
    def integrar_datos(self):
        print(f"🔍 Buscando archivos más recientes...")
        group_preffix_file = self.generate_file_groups()
        # Load the record file to check for existing processed files

        if os.path.exists(self.record_file):
            with open(self.record_file, "r") as f:
                record = json.load(f)
        else:
            record = {}

    
        for group in group_preffix_file:
            # Compute output file path
            prefix = group['group_id'].split("_")[0]   # "2025-09-19-08"
            output_file_name = f'{prefix}_Integracion.xlsx' 
            output_file_path = os.path.join(self.integration_path, output_file_name)
            file_key = os.path.abspath(output_file_path)

            # Check if file exists and matches the record
            skip_processing = False
            if file_key in record and os.path.exists(output_file_path):
                last_mod_time = os.path.getmtime(output_file_path)
                if abs(record[file_key] - last_mod_time) < 1:  # tolerance of 1 second
                    print(f"⏩ Grupo '{group['group_id']}' ya procesado y sin cambios, omitiendo procesamiento.")
                    skip_processing = True

            if skip_processing:
                continue            
            
            # Cargamos dataframes 
            df_altas    = pd.read_excel(group['altas'])    if group['altas']    else pd.DataFrame()
            df_prei     = pd.read_excel(group['prei'])     if group['prei']     else pd.DataFrame()
            df_facturas = pd.read_excel(group['facturas']) if group['facturas'] else pd.DataFrame()
            df_ordenes  = pd.read_excel(group['ordenes'])  if group['ordenes']  else pd.DataFrame()
            # Generamos fecha de grupo de archivos 
            prefix = group['group_id'].split("_")[0]   # "2025-09-19-08"
            dt = datetime.datetime.strptime(prefix, "%Y-%m-%d-%H")
            group_date = dt.replace(minute=0, second=0, microsecond=0)

            # Agregar fechas de archivo a cada DataFrame
            if not df_altas.empty:
                df_altas['file_date'] = group_date
            if not df_prei.empty:
                df_prei['file_date'] = group_date
            if not df_facturas.empty:
                df_facturas['file_date'] = group_date
            if not df_ordenes.empty:
                df_ordenes['file_date'] = group_date
            df_facturas = self.invoices_cleaning(df_facturas)
            altas_invoice_join = {'left': ['noAlta', 'noOrden'], 'right': ['Alta', 'Referencia'], 'return': ['UUID']}
            df_altas = self.populate_df(df_altas, df_facturas, altas_invoice_join)
            altas_prei_join = {'left': ['UUID'], 'right': ['Folio Fiscal'], 'return': ['Estado C.R.']}
            df_altas = self.populate_df(df_altas, df_prei, altas_prei_join)
            output_file_name = f'{prefix}_Integracion.xlsx' 
            output_file_path = os.path.join(self.integration_path, output_file_name)

            df_ordenes_and_altas = self.IMSS_ordenes_and_altas(df_ordenes, df_altas)
            # -- Validar resultados ---
            total_ordenes = df_ordenes_and_altas.drop_duplicates(subset = ['orden'])
            total_ordenes = total_ordenes['cantidadSolicitada'].sum()
            total_ordenes_df_origen = df_ordenes['cantidadSolicitada'].sum()
            total_entregas_ordenes_altas = df_ordenes_and_altas['cantRecibida'].sum()
            total_entregas_altas = df_altas['cantRecibida'].sum()
            
            if total_ordenes == total_ordenes_df_origen and total_entregas_ordenes_altas == total_entregas_altas:
                print(f"✅ Validación exitosa: total ordenes del df fusionado con altas {total_ordenes} coincide con origen {total_ordenes_df_origen}")
                print(f"✅ Validación exitosa: total entregas del df fusionado con altas {total_entregas_ordenes_altas} coincide con origen {total_entregas_altas}")
            else:
                if total_ordenes == total_ordenes_df_origen and total_entregas_ordenes_altas != total_entregas_altas: 
                    print(f"✅ Validación exitosa: total ordenes del df fusionado con altas {total_ordenes} coincide con origen {total_ordenes_df_origen}")
                    print(f"❌ Validación fallida: total entregas del df fusionado con altas {total_entregas_ordenes_altas} NO coincide con origen {total_entregas_altas}")
                elif total_ordenes != total_ordenes_df_origen and total_entregas_ordenes_altas == total_entregas_altas:
                    print(f"✅ Validación exitosa: total entregas del df fusionado con altas {total_entregas_ordenes_altas} coincide con origen {total_entregas_altas}")
                    print(f"❌ Validación fallida: total ordenes del df fusionado con altas {total_ordenes} NO coincide con origen {total_ordenes_df_origen}")
                else: 
                    print(f"❌ Validación fallida: total ordenes del df fusionado con altas {total_ordenes} NO coincide con origen {total_ordenes_df_origen}")
                    print(f"❌ Validación fallida: total entregas del df fusionado con altas {total_entregas_ordenes_altas} NO coincide con origen {total_entregas_altas}")
            
            self.save_if_modified(output_file_path, {
                "df_altas": df_altas,
                "df_prei": df_prei,
                "df_facturas": df_facturas,
                "df_ordenes": df_ordenes,
                "df_ordenes_and_altas": df_ordenes_and_altas
            }, self.record_file)
                       
    def save_if_modified(self, output_file_path, df_dict, record_file):
        """
        Guarda múltiples DataFrames en un Excel solo si el archivo destino
        no tiene la misma fecha de modificación registrada.
        """

        # 1. Cargar registro si existe
        if os.path.exists(record_file):
            with open(record_file, "r") as f:
                record = json.load(f)
        else:
            record = {}

        file_key = os.path.abspath(output_file_path)
        last_mod_time = None

        if os.path.exists(output_file_path):
            last_mod_time = os.path.getmtime(output_file_path)

        # 2. Verificar si ya está registrado y coincide
        if file_key in record and last_mod_time is not None:
            if abs(record[file_key] - last_mod_time) < 1:  # tolerancia de 1 segundo
                mod_dt = datetime.datetime.fromtimestamp(last_mod_time)
                print(f"⏩ Archivo '{os.path.basename(output_file_path)}' no ha cambiado desde {mod_dt}, no se sobrescribe.")
                return

        # 3. Escribir el archivo
        with pd.ExcelWriter(output_file_path, engine='openpyxl') as writer:
            for name, df in df_dict.items():
                if not df.empty:
                    df.to_excel(writer, sheet_name=name, index=False)
                    print(f"✅ Hoja '{name}' guardada con {len(df)} filas")

        print(f"\n🎉 ¡Integración completada exitosamente!")
        print(f"📁 Archivo guardado en: {os.path.basename(output_file_path)}")

        # 4. Actualizar registro
        new_mod_time = os.path.getmtime(output_file_path)
        record[file_key] = new_mod_time
        with open(record_file, "w") as f:
            json.dump(record, f)

    def invoices_cleaning(self, df_facturas: pd.DataFrame) -> pd.DataFrame:
        cols = ['Referencia', 'Alta']
        df_facturas = df_facturas.dropna(subset=cols)

        mask_dot00 = (
            df_facturas['Referencia'].astype(str).str.contains(r'\.00', na=False) |
            df_facturas['Alta'].astype(str).str.contains(r'\.00', na=False)
        )
        df_facturas = df_facturas[~mask_dot00]
        df_facturas = df_facturas.drop_duplicates(subset=['Referencia', 'Alta'])

        return df_facturas
    
    def populate_df(self, left_df, right_df, query_dict):
        """
        Pobla columnas en left_df a partir de right_df según query_dict.
        
        query_dict:
            {
                'left': ['col1_left', 'col2_left'],
                'right': ['col1_right', 'col2_right'],
                'return': ['colX_right', 'colY_right']
            }
        """
        left_keys = query_dict['left']
        right_keys = query_dict['right']
        return_cols = query_dict['return']

        # Validación
        if len(left_keys) != len(right_keys):
            raise ValueError("Las llaves left y right deben tener la misma longitud")
        # Validación de existencia de columnas en left_df
        missing_left = [col for col in left_keys if col not in left_df.columns]
        if missing_left:
            print(f"⚠️ Columnas faltantes en left_df: {', '.join(missing_left)}. No se puede proceder con el merge.")
            return left_df

        # Validación de existencia de columnas en right_df para keys
        missing_right_keys = [col for col in right_keys if col not in right_df.columns]
        if missing_right_keys:
            print(f"⚠️ Columnas faltantes en right_df para keys: {', '.join(missing_right_keys)}. No se puede proceder con el merge.")
            return left_df

        # Validación de existencia de columnas en right_df para return
        missing_return = [col for col in return_cols if col not in right_df.columns]
        if missing_return:
            print(f"⚠️ Columnas faltantes en right_df para return: {', '.join(missing_return)}. No se puede proceder con el merge.")
            return left_df

        # Índice compuesto para búsquedas rápidas
        right_index = right_df.groupby(right_keys)[return_cols].agg(lambda x: ','.join(x.astype(str))).reset_index()

        # Hacer merge left→right (left join)
        merged = pd.merge(
            left_df,
            right_index,
            how="left",
            left_on=left_keys,
            right_on=right_keys,
            suffixes=('', '_right')
        )

        # Rellenar NaN con "no localizado"
        for col in return_cols:
            if col in merged.columns:
                merged[col] = merged[col].fillna("no localizado")

        # Eliminar columnas auxiliares de join (las right_keys)
        merged = merged.drop(columns=right_keys, errors="ignore")

        return merged

    def run_queries(self, queries_folder, schema, table_name):
        """Ejecuta las consultas SQL en el folder especificado."""
        print(f"🔄 Ejecutando consultas en {queries_folder}...")
        for query_file in glob.glob(os.path.join(queries_folder, "*.sql")):
            with open(query_file, "r") as f:
                query = f.read()
                self.execute_query(query, schema, table_name)

if __name__ == "__main__":
    folder_root = os.getcwd()
    working_folder = os.path.join(folder_root, "Implementación")
    yaml_path = os.path.join(working_folder, "config.yaml")
    yaml_exists = os.path.exists(yaml_path)
    integration_path = os.path.join(working_folder, "Integración")
    if yaml_exists:
        # Abrir y cargar el contenido YAML en un diccionario
        with open(yaml_path, 'r', encoding='utf-8') as f:
            data_access = yaml.safe_load(f)
        print(f"✅ Archivo YAML cargado correctamente: {os.path.basename(yaml_path)}")
    app = DataIntegration(working_folder, data_access, integration_path)
    app.integrar_datos()
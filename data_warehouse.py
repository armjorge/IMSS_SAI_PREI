import os
from config import ConfigManager
from helpers import message_print, create_directory_if_not_exists

from sql_connexion_updating import SQL_CONNEXION_UPDATING
from sqlalchemy import create_engine, text
import pandas as pd
from datetime import datetime
from typing import Optional

try:
    import matplotlib.pyplot as plt
    from matplotlib.backends.backend_pdf import PdfPages
    from matplotlib.ticker import FuncFormatter  # Movido aquí para evitar reimports
    _HAS_MPL = True
except Exception:
    _HAS_MPL = False

try:
    from docx import Document
    from docx.shared import Inches
    _HAS_DOCX = True
except Exception:
    _HAS_DOCX = False

class DataWarehouse:
    def __init__(self, data_access, working_folder=None):
        self.data_access = data_access
        self.working_fol

class DataWarehouse:
    def __init__(self, data_access, working_folder=None):
        self.data_access = data_access
        self.working_folder = working_folder or os.getcwd()
        

    def generate_altas_historico_report(self, df_altas_historico: pd.DataFrame,
                                        report_folder: Optional[str] = None) -> Optional[str]:
        print("Inicio de generate_altas_historico_report")  # Print de depuración
        """
        Genera un DOCX con secciones para PTYCSA y CPI:
        - Filtra df_altas_historico por 'fechaaltatrunc' (< 2025-06-30 para PTYCSA, >= para CPI).
        - Para cada subconjunto: gráfico de barras comparativo, tabla resumen y gráfico de tendencias.
        - Selección interactiva de fechas aplicada al DataFrame completo.
        - Guarda en report_folder/consulta {YYYY} {MM} {DD}.docx
        """
        if df_altas_historico is None or df_altas_historico.empty:
            print("Sin datos para reporte")
            return None

        df = df_altas_historico.copy()
        df.info()

        # Verificar y convertir 'fechaaltatrunc' a datetime, luego a date (para coincidir con consulta SQL)
        if 'fechaaltatrunc' not in df.columns:
            raise ValueError("Se requiere 'fechaaltatrunc' para filtrar PTYCSA y CPI")
        df['fechaaltatrunc'] = pd.to_datetime(df['fechaaltatrunc'], errors='coerce').dt.date

        # Tipos para columnas comunes
        if 'file_date' not in df.columns:
            raise ValueError("Se requiere 'file_date'")
        df['file_date'] = pd.to_datetime(df['file_date'], errors='coerce').dt.date
        if 'importe' not in df.columns:
            raise ValueError("Se requiere 'importe'")
        df['importe'] = pd.to_numeric(df['importe'], errors='coerce').fillna(0)

        # Detectar columna de estado (usando df completo, asumiendo consistencia)
        def _norm(s: str) -> str:
            return ''.join(ch for ch in s.lower() if ch.isalnum() or ch == '_')
        estado_col = None
        for c in df.columns:
            if _norm(c) == 'estado_cr':
                estado_col = c
                break
        if estado_col is None and 'estado_c.r.' in df.columns:
            estado_col = 'estado_c.r.'
        if estado_col is None:
            raise ValueError("No se encontró la columna de estado (esperado 'estado_c.r.')")

        # Fechas disponibles (del DataFrame completo)
        dates = sorted(df['file_date'].dropna().unique())
        print(f"file_date.nunique = {len(dates)}")
        if not dates:
            print("No hay fechas válidas")
            return None

        # Interactive selection loop (una vez, para ambos subconjuntos)
        while True:
            print("\nFechas disponibles:")
            for i, date in enumerate(dates):
                print(f"{i}: {date}")
            try:
                current_idx = int(input("Elige índice para reporte_a_comparar (current): "))
                prev_idx = int(input("Elige índice para reporte_previo (previous): "))
                if 0 <= current_idx < len(dates) and 0 <= prev_idx < len(dates):
                    current_date = dates[current_idx]
                    prev_date = dates[prev_idx]
                    print(f"Seleccionado - Current: {current_date}, Previous: {prev_date}")
                    confirm = input("Confirmar? (y/n): ").lower()
                    if confirm == 'y':
                        break
                else:
                    print("Índices inválidos.")
            except ValueError:
                print("Entrada inválida. Usa números enteros.")

        prev_label = f"Seleccionado: {prev_date}"

        # Filtrar DataFrames por cortes (usando .date() para coincidir con consulta SQL)
        cutoff_date = pd.to_datetime('2025-06-30').date()
        df_previous_tycsa = df[(df['file_date'] == prev_date) & (df['fechaaltatrunc'] < cutoff_date)]
        df_current_tycsa = df[(df['file_date'] == current_date) & (df['fechaaltatrunc'] < cutoff_date)]
        df_previous_CPI = df[(df['file_date'] == prev_date) & (df['fechaaltatrunc'] >= cutoff_date)]
        df_current_CPI = df[(df['file_date'] == current_date) & (df['fechaaltatrunc'] >= cutoff_date)]

        print(f"\nDepuración: cutoff_date = {cutoff_date}")
        print(f"df_current_tycsa shape: {df_current_tycsa.shape}")
        print(f"df_previous_tycsa shape: {df_previous_tycsa.shape}")
        print(f"df_current_CPI shape: {df_current_CPI.shape}")
        print(f"df_previous_CPI shape: {df_previous_CPI.shape}")

        # Agrupar por estado_col y sumar importe para cada DataFrame
        grouped_previous_tycsa = df_previous_tycsa.groupby(estado_col)['importe'].sum()
        grouped_current_tycsa = df_current_tycsa.groupby(estado_col)['importe'].sum()
        grouped_previous_CPI = df_previous_CPI.groupby(estado_col)['importe'].sum()
        grouped_current_CPI = df_current_CPI.groupby(estado_col)['importe'].sum()

        print("\n=== Agrupado Previous PTYCSA (file_date == prev_date & fechaaltatrunc < cutoff) ===")
        print(grouped_previous_tycsa.head())

        print("\n=== Agrupado Current PTYCSA (file_date == current_date & fechaaltatrunc < cutoff) ===")
        print(grouped_current_tycsa.head())

        print("\n=== Agrupado Previous CPI (file_date == prev_date & fechaaltatrunc >= cutoff) ===")
        print(grouped_previous_CPI.head())

        print("\n=== Agrupado Current CPI (file_date == current_date & fechaaltatrunc >= cutoff) ===")
        print(grouped_current_CPI.head())

        # Crear y imprimir tablas resumen
        print("\n=== Summary PTYCSA (previous y current combinados) ===")
        summary_tycsa = pd.concat([grouped_previous_tycsa.rename('previous'), grouped_current_tycsa.rename('current')], axis=1).fillna(0)
        # Calcular delta y delta_pct antes de Total
        summary_tycsa['delta'] = summary_tycsa['current'] - summary_tycsa['previous']
        summary_tycsa['delta_pct'] = summary_tycsa.apply(lambda r: (r['delta']/r['previous']*100.0) if r['previous'] else None, axis=1)
        # Agregar fila de Total
        summary_tycsa.loc['Total', 'previous'] = summary_tycsa['previous'].sum()
        summary_tycsa.loc['Total', 'current'] = summary_tycsa['current'].sum()
        summary_tycsa.loc['Total', 'delta'] = summary_tycsa['delta'].sum()
        if summary_tycsa.loc['Total', 'previous'] != 0:
            summary_tycsa.loc['Total', 'delta_pct'] = (summary_tycsa.loc['Total', 'delta'] / summary_tycsa.loc['Total', 'previous']) * 100
        else:
            summary_tycsa.loc['Total', 'delta_pct'] = None
        print(summary_tycsa)

        print("\n=== Summary CPI (previous y current combinados) ===")
        summary_cpi = pd.concat([grouped_previous_CPI.rename('previous'), grouped_current_CPI.rename('current')], axis=1).fillna(0)
        # Calcular delta y delta_pct antes de Total
        summary_cpi['delta'] = summary_cpi['current'] - summary_cpi['previous']
        summary_cpi['delta_pct'] = summary_cpi.apply(lambda r: (r['delta']/r['previous']*100.0) if r['previous'] else None, axis=1)
        # Agregar fila de Total
        summary_cpi.loc['Total', 'previous'] = summary_cpi['previous'].sum()
        summary_cpi.loc['Total', 'current'] = summary_cpi['current'].sum()
        summary_cpi.loc['Total', 'delta'] = summary_cpi['delta'].sum()
        if summary_cpi.loc['Total', 'previous'] != 0:
            summary_cpi.loc['Total', 'delta_pct'] = (summary_cpi.loc['Total', 'delta'] / summary_cpi.loc['Total', 'previous']) * 100
        else:
            summary_cpi.loc['Total', 'delta_pct'] = None
        print(summary_cpi)

        # Función auxiliar para generar sección por summary
        def add_summary_section(doc, summary, subset_name, current_date, prev_date, prev_label, out_dir):
            print(f"Generando sección {subset_name}")  # Print de depuración
            if summary.empty:
                print(f"Summary {subset_name} vacío, omitiendo sección.")
                return

            # Calcular delta y delta_pct
            summary['delta'] = summary['current'] - summary['previous']
            summary['delta_pct'] = summary.apply(lambda r: (r['delta']/r['previous']*100.0) if r['previous'] else None, axis=1)
            summary = summary.sort_values('current', ascending=False)

            # Formatting
            def format_currency(x):
                return f"${x:,.2f}" if pd.notnull(x) else ""
            summary_formatted = summary.copy()
            for col in ['current', 'previous', 'delta']:
                summary_formatted[col] = summary_formatted[col].apply(format_currency)
            summary_formatted['delta_pct'] = summary_formatted['delta_pct'].apply(lambda x: f"{x:.2f}%" if pd.notnull(x) else "")

            # Subtítulo
            doc.add_heading(f"Sección {subset_name}", level=1)

            # Página: barras comparativas
            fig, ax = plt.subplots(figsize=(11.69, 8.27))
            idx = summary.index.tolist()
            x = range(len(idx))
            ax.bar([i - 0.2 for i in x], summary['previous'].values, width=0.4, label='Previous')
            ax.bar([i + 0.2 for i in x], summary['current'].values, width=0.4, label='Current')
            ax.set_xticks(list(x))
            ax.set_xticklabels(idx, rotation=45, ha='right')
            ax.set_ylabel('Importe')
            ax.set_title(f"Comparativo {subset_name} - current: {current_date} | prev: {prev_label}")
            ax.legend()
            ax.grid(axis='y', linestyle='--', alpha=0.3)
            ax.yaxis.set_major_formatter(FuncFormatter(lambda x, _: f"${x:,.0f}"))
            fig.tight_layout()
            bar_img_path = os.path.join(out_dir, f'temp_bar_{subset_name}.png')
            fig.savefig(bar_img_path)
            plt.close(fig)
            doc.add_picture(bar_img_path, width=Inches(6))

            # Página: tabla
            doc.add_heading(f'Resumen por estado {subset_name} (top)', level=2)
            table = doc.add_table(rows=1, cols=len(summary_formatted.columns) + 1)
            hdr_cells = table.rows[0].cells
            hdr_cells[0].text = 'Estado'
            hdr_cells[1].text = f'Previous ({prev_date})'
            hdr_cells[2].text = f'Current ({current_date})'
            hdr_cells[3].text = 'Delta'
            hdr_cells[4].text = 'Delta %'
            for i, row in enumerate(summary_formatted.head(30).itertuples(index=True)):
                row_cells = table.add_row().cells
                row_cells[0].text = str(row.Index)
                row_cells[1].text = str(row.previous)
                row_cells[2].text = str(row.current)
                row_cells[3].text = str(row.delta)
                row_cells[4].text = str(row.delta_pct)

            # Limpiar imagen
            try:
                os.remove(bar_img_path)
                print(f"Imagen {bar_img_path} eliminada.")
            except Exception as e:
                print(f"Error eliminando imagen {bar_img_path}: {e}")

            print(f"Sección {subset_name} generada exitosamente.")  # Print de depuración

        # Preparar carpeta y archivo
        out_dir = report_folder or os.path.join(self.working_folder, 'Reportes BI')
        create_directory_if_not_exists(out_dir)
        today = datetime.now()
        out_docx = os.path.join(out_dir, f"consulta {today.year} {today.month:02d} {today.day:02d}.docx")

        title = f"Avance de Contrarecibos en el sistema PREI - PTYCSA y CPI - current: {current_date} | prev: {prev_label}"

        print(f"_HAS_DOCX: {_HAS_DOCX}, _HAS_MPL: {_HAS_MPL}")  # Print de depuración

        if not _HAS_DOCX:
            print("python-docx no disponible. Generando CSVs en su lugar.")
            summary_tycsa.to_csv(os.path.join(out_dir, f"consulta_{today.year}{today.month:02d}{today.day:02d}_PTYCSA_summary.csv"))
            summary_cpi.to_csv(os.path.join(out_dir, f"consulta_{today.year}{today.month:02d}{today.day:02d}_CPI_summary.csv"))
            return None

        if not _HAS_MPL:
            print("Matplotlib no disponible. No se pueden generar gráficos.")
            return None

        try:
            print("Creando documento DOCX...")  # Print de depuración
            doc = Document()
            doc.add_heading(title, 0)

            # Agregar secciones para PTYCSA y CPI
            add_summary_section(doc, summary_tycsa, "PTYCSA", current_date, prev_date, prev_label, out_dir)
            add_summary_section(doc, summary_cpi, "CPI", current_date, prev_date, prev_label, out_dir)

            print(f"Guardando DOCX en: {out_docx}")  # Print de depuración
            doc.save(out_docx)
            print(f"Reporte generado: {out_docx}")
            return out_docx
        except Exception as e:
            print(f"Error generando reporte DOCX: {e}")
            return None

        

    def Business_Intelligence(self):
        source_schema = "eseotres_warehouse"
        user_input = input('Elige la base del análisis, 1) cortes jupyter lab (ciclos fiscales completos), 2) cortes mini imss (sólo 6 junio): ')
        if user_input == "1":
            source_table ='altas_jupyter_lab'
        elif user_input == "2": 
            source_table =  "altas_historicas"
        print(f"📦 Fuente: {source_schema}.{source_table}")
        print("Conectando a la base de datos SOURCE...")
        #print(self.data_access)
        # Leer desde diccionario (evita AttributeError si no es objeto)
        sql_url = self.data_access.get('sql_url') if isinstance(self.data_access, dict) else None
        target_url = self.data_access.get('sql_target') if isinstance(self.data_access, dict) else None

        if not sql_url:
            print("❌ 'sql_url' no encontrado en data_access")
            return

        # Probar conexión
        try:
            engine = create_engine(sql_url)
            with engine.connect() as conn:
                ok = conn.execute(text('SELECT 1')).scalar()
                ver = conn.execute(text('SELECT version()')).scalar()
                # Cargar tabla origen como df_source
                query = f'SELECT * FROM "{source_schema}"."{source_table}"'
                df_source = pd.read_sql_query(text(query), conn)
                self.df_source = df_source
                try:
                    print(f"📊 df_source cargado: {df_source.shape[0]} filas, {df_source.shape[1]} columnas")
                    print("Llamando a generate_altas_historico_report")  # Print de depuración
                    self.generate_altas_historico_report(df_source)  # Updated call, removed previous_mode
                except Exception as e:
                    print(f"Error en generate_altas_historico_report: {e}")  # Imprimir error real
                print(f"✅ Conexión OK (SELECT 1 => {ok})")
                print(f"🗄️ Versión servidor: {ver}")
        except Exception as e:
            print(f"❌ Error al conectar/probar la BD: {e}")
            return
        finally:
            try:
                engine.dispose()
            except Exception:
                pass

        print("Inteligencia de negocios")




if __name__ == "__main__":
    folder_root = os.getcwd()
    working_folder = os.path.join(folder_root, "Implementación")
    config_manager = ConfigManager(working_folder)
    data_access = config_manager.yaml_creation(working_folder)
    data_warehouse = DataWarehouse(data_access, working_folder)
    data_warehouse.Business_Intelligence()

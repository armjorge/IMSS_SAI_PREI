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
        self.working_folder = working_folder or os.getcwd()
        
    def generate_altas_historico_report(self, df_altas_historico: pd.DataFrame,
                                        report_folder: Optional[str] = None) -> Optional[str]:  # Removed previous_mode, specific_date
        """
        Genera un PDF con:
        - df_period (último file_date) agrupado por estado y suma de importe
        - df_previous_period basado en selección interactiva de file_date
        - Comparativo con deltas y gráfico de barras + tabla + tendencias
        Guarda en report_folder/consulta {YYYY} {MM} {DD}.pdf
        """
        if df_altas_historico is None or df_altas_historico.empty:
            print("Sin datos para reporte")
            return None

        df = df_altas_historico.copy()
        df.info()
        # Tipos
        if 'file_date' not in df.columns:
            raise ValueError("Se requiere 'file_date'")
        df['file_date'] = pd.to_datetime(df['file_date'], errors='coerce')
        if 'importe' not in df.columns:
            raise ValueError("Se requiere 'importe'")
        df['importe'] = pd.to_numeric(df['importe'], errors='coerce').fillna(0)

        # Detectar columna de estado
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

        # Fechas disponibles
        dates = sorted(df['file_date'].dropna().unique())
        print(f"file_date.nunique = {len(dates)}")
        if not dates:
            print("No hay fechas válidas")
            return None

        # Interactive selection loop
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

        # Periodo actual y previo
        df_curr = df[df['file_date'] == current_date]
        df_prev = df[df['file_date'] == prev_date]
        prev_label = f"Seleccionado: {prev_date}"

        # Agrupar por estado
        df_period = df_curr.groupby(estado_col)['importe'].sum().rename('current')
        df_prev_period = df_prev.groupby(estado_col)['importe'].sum().rename('previous')
        summary = pd.concat([df_prev_period, df_period], axis=1).fillna(0)
        summary['delta'] = summary['current'] - summary['previous']
        summary['delta_pct'] = summary.apply(lambda r: (r['delta']/r['previous']*100.0) if r['previous'] else None, axis=1)
        summary = summary.sort_values('current', ascending=False)

        # Currency formatting function
        def format_currency(x):
            return f"${x:,.2f}" if pd.notnull(x) else ""

        # Apply formatting to summary for table
        summary_formatted = summary.copy()
        for col in ['current', 'previous', 'delta']:
            summary_formatted[col] = summary_formatted[col].apply(format_currency)
        summary_formatted['delta_pct'] = summary_formatted['delta_pct'].apply(lambda x: f"{x:.2f}%" if pd.notnull(x) else "")

        # Preparar carpeta y archivo
        out_dir = report_folder or os.path.join(self.working_folder, 'Reportes BI')
        create_directory_if_not_exists(out_dir)
        today = datetime.now()
        out_docx = os.path.join(out_dir, f"consulta {today.year} {today.month:02d} {today.day:02d}.docx")

        title = f"Avance de Contrarecibos en el sistema PREI - current: {current_date} | prev: {prev_label}"

        if not _HAS_DOCX:
            # Fallback: CSVs
            path_csv = os.path.join(out_dir, f"consulta_{today.year}{today.month:02d}{today.day:02d}_summary.csv")
            summary.to_csv(path_csv)
            print(f"python-docx no disponible. Se guardó CSV: {path_csv}")
            return None

        if not _HAS_MPL:
            print("Matplotlib no disponible. No se pueden generar gráficos.")
            return None

        # After summary calculation, add time-series data for trends
        # Pivot for time-series: sum importe by file_date and estado_col
        df_trend = df.groupby([estado_col, 'file_date'])['importe'].sum().reset_index()
        df_trend_pivot = df_trend.pivot(index='file_date', columns=estado_col, values='importe').fillna(0)
        # Sort columns by total sum descending
        total_sums = df_trend_pivot.sum().sort_values(ascending=False)
        top_estados = total_sums.head(10).index.tolist()  # Top 10 for readability
        df_trend_pivot = df_trend_pivot[top_estados]

        # Construir DOCX
        try:
            doc = Document()
            doc.add_heading(title, 0)

            # Página 1: barras comparativas (save as image)
            fig, ax = plt.subplots(figsize=(11.69, 8.27))
            idx = summary.index.tolist()
            x = range(len(idx))
            ax.bar([i - 0.2 for i in x], summary['previous'].values, width=0.4, label='Previous')
            ax.bar([i + 0.2 for i in x], summary['current'].values, width=0.4, label='Current')
            ax.set_xticks(list(x))
            ax.set_xticklabels(idx, rotation=45, ha='right')
            ax.set_ylabel('Importe')
            ax.set_title(title)
            ax.legend()
            ax.grid(axis='y', linestyle='--', alpha=0.3)
            # Format y-axis as currency
            from matplotlib.ticker import FuncFormatter
            ax.yaxis.set_major_formatter(FuncFormatter(lambda x, _: f"${x:,.0f}"))
            fig.tight_layout()
            bar_img_path = os.path.join(out_dir, 'temp_bar.png')
            fig.savefig(bar_img_path)
            plt.close(fig)
            doc.add_picture(bar_img_path, width=Inches(6))

            # Página 2: tabla
            doc.add_heading('Resumen por estado (top)', level=1)
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

            # Página 3: tendencias (save as image)
            fig3, ax3 = plt.subplots(figsize=(11.69, 8.27))
            for estado in df_trend_pivot.columns:
                ax3.plot(df_trend_pivot.index, df_trend_pivot[estado], label=estado, marker='o')
            ax3.set_xlabel('File Date')
            ax3.set_ylabel('Sum of Importe')
            ax3.set_title('Tendencias de Importe por Estado (Top 10)')
            ax3.legend(bbox_to_anchor=(1.05, 1), loc='upper left')
            ax3.grid(axis='y', linestyle='--', alpha=0.3)
            # Format y-axis as currency
            ax3.yaxis.set_major_formatter(FuncFormatter(lambda x, _: f"${x:,.0f}"))
            fig3.tight_layout()
            trend_img_path = os.path.join(out_dir, 'temp_trend.png')
            fig3.savefig(trend_img_path)
            plt.close(fig3)
            doc.add_picture(trend_img_path, width=Inches(6))

            doc.save(out_docx)
            # Clean up temp images
            os.remove(bar_img_path)
            os.remove(trend_img_path)
            print(f"Reporte generado: {out_docx}")
            return out_docx
        except Exception as e:
            print(f"Error generando reporte DOCX: {e}")
            return None
        

    def Business_Intelligence(self):
        source_schema = "eseotres_warehouse"
        source_table = "altas_historicas"
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
                    #self.BI_python_altas_historico(df_source)
                    # Generate default report (previous day)
                    self.generate_altas_historico_report(df_source)  # Updated call, removed previous_mode
                    
                except Exception:
                    print("df_source cargado correctamente")
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

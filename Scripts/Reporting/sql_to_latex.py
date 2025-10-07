import os
import re
import pandas as pd
from sqlalchemy import create_engine
from colorama import Fore, Style, init
from jinja2 import Environment, FileSystemLoader
from datetime import datetime


class SQL_TO_LATEX:
    def __init__(self, working_folder, data_access, queries_folder):
        self.working_folder = working_folder
        self.data_access = data_access 
        self.queries_folder = queries_folder
        self.template_file = os.path.join('.', 'Templates', 'main_report.tex') # Root, templates. 
        self.output_folder = os.path.join(self.working_folder, 'Reportes BI')

    def sql_conexion(self):
        sql_url = self.data_access['sql_url']
        #url example: 'postgresql://arXXXrge:XXX@ep-shy-darkness-10211313-poolXXXX.tech/neondb?sslmode=require&channel_binding=require'
        try:
            engine = create_engine(sql_url)
            return engine
        except Exception as e:
            print(f"❌ Error connecting to database: {e}")
            return None
        
    def sql_to_dataframe(self): 
        engine = self.sql_conexion()  # must return a SQLAlchemy Engine
        if engine is None:
            print("❌ No se pudo obtener el engine de SQL.")
            return False
        dataframes = {}
        for file in os.listdir(self.queries_folder):
            if file.endswith(".sql"):
                name = os.path.splitext(file)[0]
                print(f"{name}")
                with open(os.path.join(self.queries_folder, file)) as f:
                    query = f.read()
                df = pd.read_sql(query, engine)
                dataframes[name] = df
        print(f"{Fore.GREEN} Extracción de dataframes finalizada. {Style.RESET_ALL}")
        return dataframes

    def generate_report(self, dataframes):
        if not dataframes:
            print("No hay dataframes disponibles para el reporte.")
            return None

        template_dir, template_name = os.path.split(self.template_file)
        env = Environment(loader=FileSystemLoader(template_dir or '.'), autoescape=False)
        template = env.get_template(template_name)

        escape_map = {
            "\\": r"\textbackslash{}",
            "&": r"\&",
            "%": r"\%",
            "$": r"\$",
            "#": r"\#",
            "_": r"\_",
            "{": r"\{",
            "}": r"\}",
            "~": r"\textasciitilde{}",
            "^": r"\textasciicircum{}",
        }
        escape_pattern = re.compile("|".join(re.escape(key) for key in escape_map))

        def _format_value(value):
            if pd.isna(value):
                return ""
            if isinstance(value, float):
                if value.is_integer():
                    value = int(value)
                return f"{value:.2f}".rstrip("0").rstrip(".")
            text = str(value)
            return escape_pattern.sub(lambda match: escape_map[match.group()], text)

        latex_tables = {}
        for name, df in dataframes.items():
            if df.empty:
                latex_tables[name] = "\\begin{center}\\textit{Sin datos disponibles}\\end{center}"
                continue
            sanitized_df = df.copy()
            sanitized_df = sanitized_df.applymap(_format_value)
            sanitized_df.columns = [_format_value(col) for col in sanitized_df.columns]
            latex_tables[name] = sanitized_df.to_latex(index=False, longtable=True, escape=False, na_rep="")

        rendered_tex = template.render(**latex_tables)

        os.makedirs(self.output_folder, exist_ok=True)
        output_filename = datetime.now().strftime('%Y%m%d_%H%M%S_report.tex')
        output_path = os.path.join(self.output_folder, output_filename)
        with open(output_path, 'w', encoding='utf-8') as tex_file:
            tex_file.write(rendered_tex)
        return output_path

    def reporting_latex_run(self):
        print(f"{Fore.CYAN}\t⚡ Ejecutando consultas SQL para generación de reportes LaTeX...{Style.RESET_ALL}")
        dataframes = self.sql_to_dataframe()
        for key, value in dataframes.items(): 
            print(key)
            print(value.head())
        self.generate_report(dataframes)
        print(f"{Fore.GREEN}\t✅ Reportes LaTeX generados exitosamente.{Style.RESET_ALL}")

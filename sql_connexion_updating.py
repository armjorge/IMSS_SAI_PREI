from data_integration import DataIntegration
import pandas as pd 
from sqlalchemy import create_engine, text, insert
import os 
import glob
import re 
from psycopg2.extras import execute_values
import numpy as np
from pandas._libs.missing import NAType
from pandas._libs.tslibs.nattype import NaTType
from colorama import Fore, Style, init


class SQL_CONNEXION_UPDATING:
    def __init__(self, integration_path, data_access):
        self.integration_path = integration_path
        self.data_access = data_access
        # Create a DataIntegration instance to use its get_newest_file method
        #self.data_integration = DataIntegration(working_folder, data_access)
    
    def sql_conexion(self):
        sql_url = self.data_access['sql_url']
        #url example: 'postgresql://arXXXrge:XXX@ep-shy-darkness-10211313-poolXXXX.tech/neondb?sslmode=require&channel_binding=require'
        try:
            engine = create_engine(sql_url)
            return engine
        except Exception as e:
            print(f"❌ Error connecting to database: {e}")
            return None


    def create_schema_if_not_exists(self, connexion, schema_name):
        """Create schema if it doesn't exist"""
        try:
            with connexion.connect() as conn:
                # Check if schema exists
                result = conn.execute(text(f"SELECT schema_name FROM information_schema.schemata WHERE schema_name = '{schema_name}'"))
                if not result.fetchone():
                    # Schema doesn't exist, create it
                    conn.execute(text(f"CREATE SCHEMA {schema_name}"))
                    conn.commit()
                    print(f"✅ Schema '{schema_name}' created successfully")
                else:
                    print(f"✅ Schema '{schema_name}' already exists")
                return True
        except Exception as e:
            print(f"❌ Error creating schema '{schema_name}': {e}")
            return False

    def postgresql_main_menu(self):
        # Tabla y extensión para todos los casos
        source_path = self.integration_path
        extension = "*.xlsx"
        ############
        # Sección para extrar archivos con altas y cargarlos a tablas. 
        ############
        sheet_name = "df_altas"
        table_name = "altas_historicas"
        primary_keys = ["noAlta", "noOrden", "file_date"]
        schema = self.data_access.get('data_warehouse_schema')
        columns_dict = {
            "drop_columns": [],
            "date_first_columns": ["fechaAltaTrunc", "fpp"],
            "int_columns": ["cantRecibida", "clasPtalRecep"],  # enteros
            "float_columns": ["importe"],  # numéricos decimales
            "string_columns": ["noOrden", "noAlta", "noContrato", "clave", "descUnidad", "uuid", "estado_c_r_"],
            "nan_columns": ["clasPtalDist", "descDist", "totalItems", "resguardo"],
        }
        altas_updating = self.postgresql_insert_or_creation(source_path,extension, sheet_name, table_name, primary_keys, schema , columns_dict)

        ############
        # Sección para extrar archivos con ordesnes_altas y sanciones, cargarlos a tablas. 
        ############
        sheet_name_integracion = "df_ordenes_and_altas"
        table_name_integracion = "ordenes_y_altas"
        primary_keys_integracion = ["orden", "file_date", "cantRecibida"]
        ordenes_altas_dict = {
            "drop_columns": [],
            "date_first_columns": ["fechaAltaTrunc"],
            "int_columns": ["cantRecibida", "days_diff", "precio", "cantidadSolicitada"],  # enteros
            "float_columns": ["cantidadSancionable", "sancion"],  # numéricos decimales
            "string_columns": ['orden', 'solicitud'],
            "nan_columns": []
        }
        
        integracion_updating = self.postgresql_insert_or_creation(source_path,extension, sheet_name_integracion, table_name_integracion, primary_keys_integracion, schema , ordenes_altas_dict)

        ############
        # Sección para extrar archivos con ordenes y sanciones, cargarlos a tablas. 
        ############
        sheet_name = "df_ordenes"
        table_name = "ordenes_historicas"
        primary_keys = ["orden", "file_date"]
        dict_SQL_types = {
            "drop_columns": [],
            "date_first_columns": ["fechaExpedicion", 'fechaEntrega'],
            "int_columns": ["cantidadSolicitada"],  # enteros
            "float_columns": [],  # numéricos decimales
            "string_columns": [],
            "nan_columns": [],
        }        
        integracion_updating = self.postgresql_insert_or_creation(source_path,extension, sheet_name, table_name, primary_keys, schema , dict_SQL_types)

    def postgresql_insert_or_creation(self, source_path,extension, sheet_name, table_name, primary_keys, schema , columns_dict):
        print(f"📂 Iniciando extracción de {sheet_name} desde archivos Excel...")
        # Buscar todos los Excel en la carpeta de integración
        xlsx_files = [
            f for f in glob.glob(os.path.join(source_path, extension))
            if not os.path.basename(f).startswith("~")
        ]
        if not xlsx_files:
            print("⚠️ No se encontraron archivos Excel en la ruta de integración.")
            return

        # Concatenar todos los df_altas de cada archivo
        drop_columns   = columns_dict.get("drop_columns")
        date_first_columns = columns_dict.get("date_first_columns")
        int_columns    = columns_dict.get("int_columns")
        float_columns  = columns_dict.get("float_columns")
        string_columns = columns_dict.get("string_columns")
        nan_columns    = columns_dict.get("nan_columns")

        df_list = []
        for file in xlsx_files:
            try:
                df = pd.read_excel(file, sheet_name=sheet_name, engine="openpyxl")
                df_list.append(df)
                print(f"\t✅ {sheet_name} de {os.path.basename(file)} con {len(df)} filas")
            except Exception as e:
                print(f"\t⚠️ No se pudo leer 'df_altas' de {file}: {e}")

        if not df_list:
            print(f"\t⚠️ Ninguna hoja {sheet_name} pudo ser cargada.")
            return

        df_altas = pd.concat(df_list, ignore_index=True)
        if drop_columns:  # solo entra si la lista no está vacía
            df_altas = df_altas.drop(columns=drop_columns, errors="ignore")        
        df_altas = df_altas.loc[:, ~df_altas.columns.str.contains("^Unnamed", case=False)]
        #print(df_altas.info())

        # Nan Columns0
        if nan_columns:
            for col in nan_columns:
                if col in df_altas.columns:
                    if col in ['clasPtalDist', 'totalItems', 'resguardo']:
                        df_altas[col] = pd.to_numeric(df_altas[col], errors="coerce").astype('Float64')
                    else:  # strings (ej. descDist)
                        df_altas[col] = df_altas[col].astype('string').str.strip()
                        df_altas[col] = df_altas[col].replace({'nan': pd.NA, 'NaN': pd.NA, 'None': pd.NA})

        # Convertir fechas (aceptar distintos formatos)
        if date_first_columns:
            for col in date_first_columns:
                if col not in df_altas.columns:
                    continue

                series = df_altas[col]

                if pd.api.types.is_datetime64_any_dtype(series):
                    continue

                parsed = pd.to_datetime(series, errors="coerce", dayfirst=True)

                needs_fallback = parsed.isna() & series.notna()
                if needs_fallback.any():
                    parsed.loc[needs_fallback] = pd.to_datetime(series.loc[needs_fallback], errors="coerce")

                df_altas[col] = parsed

        # Convertir a enteros (mantener dtype entero nullable)
        if int_columns:
            for col in int_columns:
                if col in df_altas.columns:
                    df_altas[col] = pd.to_numeric(df_altas[col], errors="coerce").astype('Int64')

        # Convertir a floats (mantener dtype flotante)
        if float_columns:
            for col in float_columns:
                if col in df_altas.columns:
                    df_altas[col] = pd.to_numeric(df_altas[col], errors="coerce").astype('Float64')

        # Convertir a string (y limpiar "nan"/"None")
        if string_columns: 
            for col in string_columns:
                if col in df_altas.columns:
                    df_altas[col] = df_altas[col].astype('string').str.strip()
                    df_altas[col] = df_altas[col].replace({'nan': pd.NA, 'NaN': pd.NA, 'None': pd.NA})

        df_altas = self.force_sql_safe_types(df_altas)
        exit_updating = self.update_postresql(df_altas, schema, table_name, primary_keys)

        return exit_updating
    
    
    def force_sql_safe_types(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Garantiza que los valores sean SQL-safe:
        - numpy.int64 -> int
        - numpy.float64 -> float
        - pd.Timestamp -> datetime.datetime
        - NaN / NaT / pd.NA -> None
        - strings -> str limpio
        Tambien detecta columnas que aun contienen 'NaT' como string.
        """
        null_markers = {"", "nat", "nan", "none", "null", "n/a", "<na>"}

        def convert_cell(x):
            if x is None:
                return None
            if isinstance(x, (NAType, NaTType)):
                return None
            if pd.isna(x):  # cubre NaN, NaT, pd.NA
                return None
            if isinstance(x, (np.integer,)):
                return int(x)
            if isinstance(x, (np.floating,)):
                return float(x)
            if isinstance(x, pd.Timestamp):
                return x.to_pydatetime()
            if isinstance(x, str):
                cleaned = x.strip()
                lowered = cleaned.lower()
                if lowered in null_markers:
                    return None
                return cleaned
            return x

        # Aplicamos la normalizacion a todo el DataFrame
        for col in df.columns:
            df[col] = df[col].apply(convert_cell)

        return df
    
    
    def _normalize_identifier(self, name: str) -> str:
        # Forzar string
        name = str(name).strip().lower()
        # Reemplazar cualquier caracter no alfanumérico por "_"
        name = re.sub(r'[^a-z0-9_]', '_', name)
        # Evitar doble guion bajo
        name = re.sub(r'_+', '_', name)
        # Si empieza con número, prefijar con "col_"
        if re.match(r'^[0-9]', name):
            name = "col_" + name
        return name

    def _map_dtype_to_pg(self, dtype: str) -> str:
        # pandas dtype as string -> PG type
        d = dtype.lower()
        # nullable integer dtype in pandas can be 'int64' or 'int64'/'int32' or 'int'/'int64'/'Int64'
        if d in ("int64", "int32") or d == "int":
            return "BIGINT"
        if d in ("float64", "float32", "float"):
            return "DOUBLE PRECISION"
        if d.startswith("datetime64[ns"):
            # covers datetime64[ns] and datetime64[ns, tz]
            return "TIMESTAMP"
        if d == "bool":
            return "BOOLEAN"
        # fallback for 'object', 'string', 'category', etc.
        return "TEXT"

    def table_creation(self, conn, df_to_upload: pd.DataFrame, schema_name: str, table_name: str, primary_keys: list):
        # Normalize column names
        norm_cols = [ self._normalize_identifier(c) for c in df_to_upload.columns ]
        # Build column defs
        col_defs = []
        for col_name, dtype in zip(norm_cols, df_to_upload.dtypes.astype(str)):
            pg_type = self._map_dtype_to_pg(dtype)
            col_defs.append(f"{col_name} {pg_type}")

        # Normalize PKs and validate they exist
        norm_pks = [ self._normalize_identifier(pk) for pk in primary_keys ]
        missing_pks = [pk for pk in norm_pks if pk not in norm_cols]
        if missing_pks:
            raise ValueError(f"Primary keys not present in DataFrame columns after normalization: {missing_pks}")

        pk_clause = f", PRIMARY KEY ({', '.join(norm_pks)})" if norm_pks else ""

        create_sql = f"""
        CREATE TABLE IF NOT EXISTS {schema_name}.{table_name} (
            {', '.join(col_defs)}
            {pk_clause}
        )
        """
        conn.execute(text(create_sql))
        print(f"✅ Tabla '{schema_name}.{table_name}' creada con PK {norm_pks}")

    def update_postresql(self, df_to_upload: pd.DataFrame, schema: str, table_name: str, primary_keys: list):
        engine = self.sql_conexion()  # must return a SQLAlchemy Engine
        if engine is None:
            print("❌ No se pudo obtener el engine de SQL.")
            return False

        # Ensure DataFrame columns are normalized the same way they’ll be created in SQL
        df_to_upload = df_to_upload.copy()
        df_to_upload.columns = [ self._normalize_identifier(c) for c in df_to_upload.columns ]
        norm_pks = [ self._normalize_identifier(pk) for pk in primary_keys ]

        try:
            with engine.begin() as conn:
                conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema}"))
                exists = conn.execute(
                    text("""
                        SELECT EXISTS (
                            SELECT 1 FROM information_schema.tables
                            WHERE table_schema = :schema AND table_name = :table
                        )
                    """),
                    {"schema": schema, "table": table_name}
                ).scalar()

                if not exists:
                    self.table_creation(conn, df_to_upload, schema, table_name, norm_pks)

                print(f"{Fore.CYAN}⚡\tPreparado para insertar datos en {schema}.{table_name}{Style.RESET_ALL}")

                # 👉 usar la misma conn aquí
                self.upsert_dataframe(conn, df_to_upload, schema, table_name, primary_keys)
            return True

        except Exception as e:
            print(f"❌ Error en update_sql: {e}")
            return False
    

    def upsert_dataframe(self, conn, df: pd.DataFrame, schema: str, table_name: str, primary_keys: list):
        df = df.copy()
        df.columns = [self._normalize_identifier(c) for c in df.columns]
        norm_pks = [self._normalize_identifier(pk) for pk in primary_keys]

        # Ensure PKs exist
        missing = [pk for pk in norm_pks if pk not in df.columns]
        if missing:
            raise ValueError(f"Primary keys not found in DataFrame columns: {missing}")

        # Drop duplicates on PK
        df = df.drop_duplicates(subset=norm_pks, keep="last")
        # Work with object dtype so None assignments stick even on datetime columns
        df = df.astype(object)
        # Normalize NULL-like values to proper None before SQL insert
        df = df.where(pd.notnull(df), None)

        # Detect lingering string "NaT" values column by column to avoid SQL errors
        nat_columns = []
        nat_samples = []
        for col in df.columns:
            col_series = df[col]

            def _is_nat_like(value):
                if isinstance(value, str):
                    return value.strip().lower() == "nat"
                if isinstance(value, NaTType):
                    return True
                if isinstance(value, np.datetime64):
                    return pd.isna(value)
                if isinstance(value, pd.Timestamp):
                    return pd.isna(value)
                return False

            mask_nat = col_series.apply(_is_nat_like)
            if mask_nat.any():
                nat_columns.append(col)
                if df[col].dtype != object:
                    df[col] = df[col].astype(object)
                df.loc[mask_nat, col] = None
                if norm_pks:
                    sample_rows = df.loc[mask_nat, norm_pks].astype(str)
                    preview = sample_rows.apply(lambda row: " | ".join(row.tolist()), axis=1).head(3).tolist()
                    nat_samples.extend(preview)

        if nat_columns:
            lingering_nat = []
            for col in nat_columns:
                mask_string_nat = df[col].apply(lambda v: isinstance(v, str) and v.strip().lower() == "nat")
                if mask_string_nat.any():
                    lingering_nat.append(col)
                    if df[col].dtype != object:
                        df[col] = df[col].astype(object)
                    df.loc[mask_string_nat, col] = None
            if lingering_nat:
                print(f"Warning: columnas aun contienen texto 'NaT' despues de la limpieza: {lingering_nat}")
            print(f"Warning: columnas con texto 'NaT' detectadas: {nat_columns}. Convirtiendo a NULL antes de insertar.")
            if nat_samples:
                print(f"   PK de ejemplo con 'NaT': {nat_samples}")

        df = df.replace({'NaT': None, 'nat': None, 'NaT ': None}, regex=False)

        def _coerce_sql_value(value):
            if value is None:
                return None
            if isinstance(value, NaTType):
                return None
            if isinstance(value, str):
                lowered = value.strip().lower()
                if lowered in {'nat', 'nan', 'none', 'null', ''}:
                    return None
                return value.strip()
            if isinstance(value, pd.Timestamp):
                return value.to_pydatetime()
            if isinstance(value, np.datetime64):
                if pd.isna(value):
                    return None
                return pd.to_datetime(value).to_pydatetime()
            if pd.isna(value):
                return None
            if isinstance(value, np.integer):
                return int(value)
            if isinstance(value, np.floating):
                if np.isnan(value):
                    return None
                return float(value)
            return value

        for col in df.columns:
            cleaned = df[col].apply(_coerce_sql_value)
            df[col] = cleaned.astype(object)

        residual_nat = []
        for col in df.columns:
            has_nat_like = df[col].apply(lambda v: (isinstance(v, str) and v.strip().lower() == "nat") or isinstance(v, NaTType) or (isinstance(v, np.datetime64) and pd.isna(v))).any()
            if has_nat_like:
                df[col] = df[col].astype(object)
                df[col] = df[col].apply(lambda v: None if (isinstance(v, str) and v.strip().lower() == "nat") or isinstance(v, NaTType) or (isinstance(v, np.datetime64) and pd.isna(v)) else v)
                if df[col].apply(lambda v: isinstance(v, str) and v.strip().lower() == "nat").any():
                    residual_nat.append(col)
        if residual_nat:
            raise ValueError(f"String 'NaT' values remain after sanitizing columns: {residual_nat}")

        # Ensure datetime columns are python datetime objects (not pandas NaT)
        for col in df.columns:
            if pd.api.types.is_datetime64_any_dtype(df[col]):
                df[col] = df[col].apply(lambda v: v.to_pydatetime() if pd.notnull(v) else None)

        #print("🔎 Column dtypes before fix:")
        #print(df.dtypes)
        #print("🔎 Sample row after conversion:")
        #print(df.iloc[0].to_dict())

        cols = list(df.columns)
        col_list_sql = ", ".join(cols)
        pk_list_sql  = ", ".join(norm_pks)

        insert_sql = f"""
            INSERT INTO {schema}.{table_name} ({col_list_sql})
            VALUES %s
            ON CONFLICT ({pk_list_sql})
            DO NOTHING
        """

        total = len(df)
        if total == 0:
            print(f"{Fore.YELLOW}⏩ No hay filas para insertar en {schema}.{table_name}.{Style.RESET_ALL}")
            return

        # 👉 Usar la conexión psycopg2 real que SQLAlchemy administra
        raw_conn = conn.connection
        cur = raw_conn.cursor()
        try:
            # Ejecutar todo en un solo batch
            sanitized_rows = (
                tuple(_coerce_sql_value(value) for value in row)
                for row in df.itertuples(index=False, name=None)
            )
            execute_values(cur, insert_sql, sanitized_rows, page_size=10000)
        finally:
            cur.close()  # commit y close los maneja SQLAlchemy

        print(f"{Fore.GREEN}✅ {total} filas insertadas en {schema}.{table_name} (ON CONFLICT DO NOTHING){Style.RESET_ALL}")

    def run_queries(self, queries_folder): 
        # Get a list of all SQL files in the queries folder
        sql_files = glob.glob(os.path.join(queries_folder, "*.sql"))
        if not sql_files:
            print(f"⚠️ No SQL files found in {queries_folder}")
            return False

        print(f"🔍 Found {len(sql_files)} SQL files: {[os.path.basename(f) for f in sql_files]}")

        connexion = self.sql_conexion()
        if connexion is None:
            return False

        try:
            for sql_file in sql_files:
                try:
                    print(f"📄 Executing query from: {os.path.basename(sql_file)}")
                    with open(sql_file, 'r', encoding='utf-8') as f:
                        query = f.read().strip()
                        
                        # Skip empty files
                        if not query:
                            print(f"⚠️ Empty file: {os.path.basename(sql_file)}")
                            continue
                        
                        # Execute the query directly without comment filtering
                        with connexion.connect() as conn:
                            result = conn.execute(text(query))
                            
                            # If it's a SELECT query, fetch and display results
                            if query.strip().upper().startswith('SELECT') or 'SELECT' in query.upper():
                                try:
                                    rows = result.fetchall()
                                    columns = list(result.keys())
                                    
                                    print(f"✅ Query returned {len(rows)} rows")
                                    print("=" * 60)
                                    
                                    if rows:
                                        self._display_grouped_results(rows, columns)
                                    else:
                                        print("✅ Query executed successfully - No rows returned")
                                        
                                except Exception as fetch_error:
                                    print(f"❌ Error fetching results: {fetch_error}")
                                    
                            else:
                                # For non-SELECT queries
                                conn.commit()
                                print(f"✅ Query executed successfully")
                                
                except Exception as e:
                    print(f"❌ Error executing query from {os.path.basename(sql_file)}: {e}")
                    continue
                    
            print("🏁 All queries completed")
            return True
            
        except Exception as e:
            print(f"❌ General error in run_queries: {e}")
            return False
            
        finally:
            if connexion:
                connexion.dispose()

    def _display_grouped_results(self, rows, columns):
        """
        Display query results in a grouped, hierarchical format for better readability.
        Detects common grouping patterns and formats them appropriately.
        """
        current_group = None
        total_amount = 0.0
        
        for row in rows:
            row_dict = dict(zip(columns, row))
            
            # Detect if this is a grouped result (common patterns)
            is_subtotal = any('subtotal' in str(value).lower() for value in row_dict.values())
            is_grand_total = any('grand total' in str(value).lower() for value in row_dict.values())
            
            # Get the first column as potential group identifier
            first_col = columns[0]
            group_value = row_dict[first_col]
            
            if is_grand_total:
                # Grand total - show at the end with emphasis
                print("\n" + "="*40)
                for col, value in row_dict.items():
                    if value and str(value).strip() and 'grand total' not in str(value).lower():
                        print(f"🏆 TOTAL GENERAL: {value}")
                print("="*40)
                
            elif is_subtotal:
                # Subtotal - show with indentation
                for col, value in row_dict.items():
                    if 'subtotal' in str(value).lower():
                        continue
                    if value and str(value).strip() and col != first_col:
                        print(f"   📊 Subtotal: {value}")
                print()  # Add spacing after subtotal
                
            else:
                # Find the detail field (estado, unidad_operativa, etc.) and the amount
                detail_field = None
                amount_field = None
                
                for col, value in row_dict.items():
                    if col != first_col and value and str(value).strip():
                        # Look for detail fields (estado, unidad_operativa)
                        if col.lower() in ['estado', 'unidad_operativa'] and not any(keyword in str(value).lower() for keyword in ['subtotal', 'grand total']):
                            detail_field = str(value).strip()  # Trim whitespace
                        # Look for amount fields
                        elif ('importe' in col.lower() or 'total' in col.lower()) and '$' in str(value):
                            amount_field = value
                
                # Check if this is a simple case (no detail field, just group and amount)
                if not detail_field and amount_field:
                    # Simple case: show group and amount on same line
                    print(f"📅 {group_value.upper()}: {amount_field}")
                else:
                    # Check for simple 3-column fallback
                    other_values = [str(v).strip() for k, v in row_dict.items() if k != first_col and v and str(v).strip()]
                    if len(other_values) == 2:
                        # Assume first is detail, second is amount
                        detail = other_values[0]
                        try:
                            amount = float(other_values[1])
                            formatted_amount = f"${amount:,.2f}"
                            total_amount += amount
                        except ValueError:
                            formatted_amount = other_values[1]
                        print(f"📅 {group_value.upper()} : {detail} {formatted_amount}")
                    else:
                        # Complex case: show hierarchical format
                        # Check if we're starting a new group
                        if group_value != current_group and not str(group_value).strip().startswith(' '):
                            current_group = group_value
                            print(f"\n📅 {group_value.upper()}")
                        
                        # Display the detail line
                        if detail_field and amount_field:
                            print(f"   • {detail_field}: {amount_field}")
                        elif detail_field:
                            print(f"   • {detail_field}")
                        elif amount_field:
                            print(f"   • {amount_field}")
                        else:
                            # Fallback for other patterns
                            if other_values:
                                print(f"   • {' | '.join(other_values)}")
        
        # Print total if there were amounts
        if total_amount > 0:
            print(f"\n🏆 TOTAL: ${total_amount:,.2f}")
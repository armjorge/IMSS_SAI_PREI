from data_integration import DataIntegration
import pandas as pd 
from sqlalchemy import create_engine, text
import os 
import glob

class SQL_CONNEXION_UPDATING:
    def __init__(self, working_folder, data_access):
        self.working_folder = working_folder
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

    def sql_column_correction(self, df_to_upload):
        df_to_upload.columns = df_to_upload.columns.str.replace(' ', '_').str.lower()
        return df_to_upload

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

    def update_sql(self, df_to_upload, schema, table_name):
        connexion = None
        try:
            connexion = self.sql_conexion()
            if connexion is None:
                return False
            # Create schema if it doesn't exist
            if not self.create_schema_if_not_exists(connexion, schema):
                print(f"⚠️ Could not create schema '{schema}', using default schema")
                schema = None
            
            df_to_upload.to_sql(table_name, con=connexion, schema=schema, if_exists='replace', index=False)
            
            schema_display = schema if schema else "default"
            print(f"✅ Successfully uploaded {len(df_to_upload)} rows to {schema_display}.{table_name}")
            return True
            
        except Exception as e:
            print(f"❌ Error updating SQL: {e}")
            return False
        finally:
            if connexion:
                connexion.dispose()

    def run_queries(self, queries_folder, schema, table_name): 
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
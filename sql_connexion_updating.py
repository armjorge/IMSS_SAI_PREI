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
                                    
                                    print(f"✅ Query returned {len(rows)} rows:")
                                    print(f"📊 Columns: {columns}")
                                    
                                    if rows:
                                        print("-" * 80)
                                        
                                        # Display first 3 rows with better formatting
                                        for i, row in enumerate(rows[:20]):
                                            row_dict = dict(zip(columns, row))
                                            for col, value in row_dict.items():
                                                print(f"  {col}: {value}")
                                            print("-" * 40)
                                            
                                        if len(rows) > 20:
                                            print(f"... and {len(rows) - 3} more rows")
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
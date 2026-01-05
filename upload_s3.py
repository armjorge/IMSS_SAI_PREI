import os 
from colorama import Fore, init, Style
import sys
from dotenv import load_dotenv
import re 
import pandas as pd
import boto3
import unicodedata

class UPLOAD_S3:
    def __init__(self, working_folder):
        init(autoreset=True)
        print(Fore.BLUE + f"\tSTARTING THE AMAZON BUCKET S3 UPLOADER" + Style.RESET_ALL)
        self.working_folder = working_folder

    def run(self):
        SAI_ORDERS_PATH = os.path.join(self.working_folder, 'SAI', 'SAI Orders_files')
        SAI_ALTAS_PATH = os.path.join(self.working_folder, 'SAI', 'SAI Altas_files')
        PREI_PATH = os.path.join(self.working_folder, 'PREI', 'PREI_files')
        IMSS_INVOICES_PATH = os.path.join(self.working_folder, 'Facturas', 'Consultas')
        SAI_ORDERS_FILES = []
        SAI_ALTAS_FILES = []
        PREI_FILES = []
        IMSS_INVOICES_FILES = []

        date_pattern = r"^\d{4}-\d{2}-\d{2}-\d{2}"

        def get_clean_files(directory):
            if not os.path.exists(directory):
                return []
            
            return [
                os.path.join(directory, f) for f in os.listdir(directory)
                if f.endswith('.xlsx')               # Must be Excel
                and not f.startswith('~')            # Ignore temp/open files
                and re.match(date_pattern, f)        # Must start with yyyy-mm-dd-hh
            ]

        # Populate Lists with full routes
        SAI_ORDERS_FILES = get_clean_files(SAI_ORDERS_PATH)
        SAI_ALTAS_FILES = get_clean_files(SAI_ALTAS_PATH)
        PREI_FILES = get_clean_files(PREI_PATH)
        IMSS_INVOICES_FILES = get_clean_files(IMSS_INVOICES_PATH)

        LOCAL_FILES_GROUPS = {'SAI_ORDERS_FILES': SAI_ORDERS_FILES, 'SAI_ALTAS_FILES': SAI_ALTAS_FILES, 'PREI_FILES': PREI_FILES, 'IMSS_INVOICES_FILES': IMSS_INVOICES_FILES}

        # FILES CHECK
        for key, value in LOCAL_FILES_GROUPS.items():
            print(f'Found {len(value)} {key}')
        # GET AWS S3 keys
        S3_KEY = os.getenv('S3_KEY_ID')
        S3_SECRET_ACCESS = os.getenv('S3_SECRET_ACCESS_KEY')
        # S3 Paths (corrected for proper structure)
        BUCKET = 'eseotres-pharma-data-storage'
        BASE_PREFIX = 'imss_ordinario/'
        S3_ALTAS_PATH = BASE_PREFIX + 'sai/sai_altas/'
        S3_ORDERS_PATH = BASE_PREFIX + 'sai/sai_orders/'
        S3_PREI_PATH = BASE_PREFIX + 'prei/'
        S3_INVOICES_PATH = BASE_PREFIX + 'invoices/'
        # Initialize S3 client
        s3 = boto3.client('s3', aws_access_key_id=S3_KEY, aws_secret_access_key=S3_SECRET_ACCESS)
        # Function to get S3 files (basenames) under a prefix
        def get_s3_files(prefix):
            response = s3.list_objects_v2(Bucket=BUCKET, Prefix=prefix)
            if 'Contents' in response:
                return [obj['Key'].split('/')[-1] for obj in response['Contents'] if obj['Key'].endswith('.csv')]
            return []
        # Get S3 files 
        S3_SAI_ORDERS_FILES = get_s3_files(S3_ORDERS_PATH)
        S3_SAI_ALTAS_FILES = get_s3_files(S3_ALTAS_PATH)
        S3_PREI_FILES = get_s3_files(S3_PREI_PATH)
        S3_IMSS_INVOICES_FILES = get_s3_files(S3_INVOICES_PATH)
        # 
        # Function to populate the S3 files from those csv files stored in S3 bucket
        # Compare the filename without extension from each group:
        # S3_SAI_ORDERS_FILES vs SAI_ORDERS_FILES, only basenames without extensions
        SAI_ORDERS_TO_UPLOAD = [] #Full path of local xlsx file without equivalent in S3 Bucket
        SAI_ALTAS_TO_UPLOAD = []
        PREI_TO_UPLOAD = []
        IMSS_INVOICES_TO_UPLOAD = []

        # Compute files to upload for each group
        sai_orders_s3_basenames = {os.path.splitext(f)[0] for f in S3_SAI_ORDERS_FILES}
        SAI_ORDERS_TO_UPLOAD = [f for f in SAI_ORDERS_FILES if os.path.splitext(os.path.basename(f))[0] not in sai_orders_s3_basenames]

        sai_altas_s3_basenames = {os.path.splitext(f)[0] for f in S3_SAI_ALTAS_FILES}
        SAI_ALTAS_TO_UPLOAD = [f for f in SAI_ALTAS_FILES if os.path.splitext(os.path.basename(f))[0] not in sai_altas_s3_basenames]

        prei_s3_basenames = {os.path.splitext(f)[0] for f in S3_PREI_FILES}
        PREI_TO_UPLOAD = [f for f in PREI_FILES if os.path.splitext(os.path.basename(f))[0] not in prei_s3_basenames]

        invoices_s3_basenames = {os.path.splitext(f)[0] for f in S3_IMSS_INVOICES_FILES}
        IMSS_INVOICES_TO_UPLOAD = [f for f in IMSS_INVOICES_FILES if os.path.splitext(os.path.basename(f))[0] not in invoices_s3_basenames]
        def remove_accents(text):
            return ''.join(
                c for c in unicodedata.normalize('NFD', text)
                if unicodedata.category(c) != 'Mn'
            ).lower().strip()
        def convert_xlsx_to_csv(xlsx_files: list) -> list: 
            csv_files = []
            for xlsx_file in xlsx_files: 
                # Read Excel
                df = pd.read_excel(xlsx_file)
                # Lowercase and strip headers
                df.columns = [remove_accents(col) for col in df.columns]                # Clean fields: remove problematic characters from string cells
                def clean_cell(cell):
                    if isinstance(cell, str):
                        cell = cell.replace('"', '')  # Remove double quotes
                        cell = cell.replace("'", '')  # Remove single quotes
                        cell = cell.replace(',', '')  # Remove commas (to prevent breaking CSV; replace with '' as per instructions)

                    return cell
                df = df.map(clean_cell)
                # Create CSV path (same name, change extension)
                csv_path = os.path.splitext(xlsx_file)[0] + '.csv'
                # Write to CSV (pandas handles quoting automatically)
                df.to_csv(csv_path, index=False)
                csv_files.append(csv_path)
            return csv_files
        
        WORKLOAD_BATCH = [
            {'SAI_ORDERS_TO_UPLOAD': {'files': SAI_ORDERS_TO_UPLOAD, 's3': S3_ORDERS_PATH}}, 
            {'SAI_ALTAS_TO_UPLOAD': {'files': SAI_ALTAS_TO_UPLOAD, 's3': S3_ALTAS_PATH}},
            {'PREI_TO_UPLOAD': {'files': PREI_TO_UPLOAD, 's3': S3_PREI_PATH}},
            {'IMSS_INVOICES_TO_UPLOAD': {'files': IMSS_INVOICES_TO_UPLOAD, 's3': S3_INVOICES_PATH}}
            ]

        for item in WORKLOAD_BATCH:
            for key, values in item.items():
                if len(values['files']):
                    print(Fore.GREEN  +f'\tFound {len(values['files'])} files to process and upload to {key}')
                    # Clean the files to convert them to csv
                    csv_files = convert_xlsx_to_csv(values['files'])
                    # upload to the target s3 bucket
                    for csv in csv_files:
                        basename = os.path.basename(csv)
                        s3_key = values['s3'] + basename
                        s3.upload_file(csv, BUCKET, s3_key)
                        print(f'\tUploaded {os.path.basename(csv)} to s3://{BUCKET}/{s3_key}')
                    # Clean up local CSV files (optional but recommended)
                    for csv in csv_files:
                        os.remove(csv)
                else: 
                    print(Fore.YELLOW  +f'\tNot new files to process and upload to {key}' )



if __name__ == "__main__":
    BASE_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__)))
    # Aseguramos que BASE_PATH esté en sys.path
    if BASE_PATH not in sys.path:
        sys.path.insert(0, BASE_PATH) 
    env_file = os.path.join(BASE_PATH, ".env")
    folder_name = "MAIN_PATH"
    working_folder = BASE_PATH
    if os.path.exists(env_file):
        # Modo desarrollo local: leemos .env
        load_dotenv(dotenv_path=env_file)
        env_main_path = os.getenv(folder_name)
        if env_main_path:
            working_folder = env_main_path
            print(f"✅ MAIN_PATH tomado desde .env: {working_folder}")
        else:
            print(
                f"⚠️ Se encontró .env en {env_file} pero la variable {folder_name} no está definida.\n"
                f"Se usará BASE_PATH como working_folder: {working_folder}"
            )

    UPLOAD_S3(working_folder).run()
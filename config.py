import os
import yaml
from helpers import message_print, create_directory_if_not_exists

class ConfigManager:
    def __init__(self, working_folder):
        self.working_folder = working_folder
        self.config_path = os.path.join(working_folder, "config.yaml")

    def yaml_creation(self, working_folder): 
        output_yaml = self.config_path
        yaml_exists = os.path.exists(output_yaml)

        if yaml_exists:
            # Abrir y cargar el contenido YAML en un diccionario
            with open(output_yaml, 'r', encoding='utf-8') as f:
                data_access = yaml.safe_load(f)
            print(f"✅ Archivo YAML cargado correctamente: {os.path.basename(output_yaml)}")
            return data_access

        else: 
            print(message_print("No se localizó un yaml válido, vamos a crear uno"))
            
            # Crear directorio si no existe
            create_directory_if_not_exists(working_folder)
            
            platforms = ["imss", "prei"] # Los items
            fields    = ["url", "user", "password", "actions"] # Cada variable
            
            lines = []
            for platform in platforms:
                for field in fields:
                    # clave = valor vacío
                    lines.append(f"{platform}_{field}: ''")
                lines.append("")  # línea en blanco entre bloques
            
            # Escribe el archivo YAML
            with open(output_yaml, "w", encoding="utf-8") as f:
                f.write("\n".join(lines))
            print(message_print("Generamos el YAML para que captures información, vuelve a correr la script para abrirlo."))
            return None
import os

def message_print(message):
    """Formatea mensajes con asteriscos para destacarlos"""
    message_highlights = '*' * len(message)
    return f'\n{message_highlights}\n{message}\n{message_highlights}\n'

def create_directory_if_not_exists(path_or_paths):
    """Crea directorios si no existen"""
    if isinstance(path_or_paths, str):
        paths = [path_or_paths]
    else:
        paths = path_or_paths
    
    for path in paths:
        if not os.path.exists(path):
            print(f"\tCreando directorio: {os.path.basename(path)}")
            os.makedirs(path)
        else:
            print(f"\tDirectorio encontrado: {os.path.basename(path)}")

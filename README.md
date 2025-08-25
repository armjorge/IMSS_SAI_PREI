# mini_imss

Sistema para descargar las altas y su estatus del IMSS.
## Descripción

Mini IMSS es una herramienta automatizada que permite la descarga y procesamiento de información sobre altas de entregas y su estatus de contrarecibo en el Instituto Mexicano del Seguro Social (IMSS) durante un período semestral específico.

## Características

- ✅ Descarga automática de datos de altas IMSS
- ✅ Consulta de estatus de contrarecibo
- ✅ Extrae información de PAQ que liga orden, alta y factura. 
- ✅ Carga a SQL
- ✅ Generación de reportes 

## Instalación

```bash
# Clonar el repositorio
git clone https://github.com/armjorge/IMSS_SAI_PREI.git
cd mini_imss

# Instalar dependencias (si aplica)
pip install -r requirements.txt
# o
npm install
```

## Uso

1. Configurar las credenciales de acceso al sistema IMSS/CPI
2. Especificar el período semestral a consultar
3. Ejecutar el proceso de descarga
4. Revisar los reportes generados

## Estructura del Proyecto

```
IMSS_SAI_PREI/
├── README.md
├── Implementación/ # Se genera esta carpeta al iniciar

```

## Requisitos

- Acceso autorizado al sistema IMSS/CPI
- Credenciales válidas
- Conexión a internet estable


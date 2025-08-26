# IMSS SAI y PREI - Sistema Integrado de Facturación

Sistema automatizado para descargar, procesar e integrar información de altas IMSS (SAI) y contrarecibos (PREI) con datos de facturación.

## Descripción

Mini IMSS es una herramienta completa que automatiza la descarga y procesamiento de información del Instituto Mexicano del Seguro Social (IMSS), integrando datos de diferentes fuentes para generar reportes consolidados de facturación.

### Módulos Principales
- **SAI (Sistema de Altas IMSS)**: Descarga automática de altas de entregas
- **PREI (Plataforma de Reportes)**: Consulta de estatus de contrarecibo
- **Integración de Facturas**: Procesamiento de archivos PAQ que ligan órdenes, altas y facturas
- **Base de Datos**: Carga y consulta en PostgreSQL
- **Reportes**: Generación automática de análisis y agrupaciones

## Características

- ✅ **Descarga Automatizada**: Selenium WebDriver para SAI y PREI
- ✅ **Gestión de Archivos**: Organización automática por fechas y tipos
- ✅ **Integración de Datos**: Combina altas, contrarecibos y facturas en un dataset único
- ✅ **Base de Datos**: Carga automática a PostgreSQL con esquemas personalizados
- ✅ **Reportes SQL**: Consultas predefinidas para análisis de estatus y importes
- ✅ **Interfaz Interactiva**: Menú de opciones para ejecutar procesos individuales o completos

## Instalación

### Requisitos Previos
- Python 3.8+
- Google Chrome (se instala automáticamente la versión de testing)
- Acceso a sistemas IMSS/PREI
- Base de datos PostgreSQL (opcional)

### Pasos de Instalación

```bash
# 1. Clonar el repositorio
git clone https://github.com/armjorge/IMSS_SAI_PREI.git
cd mini_imss

# 2. Instalar dependencias Python
pip install -r requirements.txt

# 3. Ejecutar por primera vez para generar configuración
python main.py
```

### Configuración Inicial
Al ejecutar por primera vez, se generará un archivo `config.yaml` que debes completar:

```yaml
# Credenciales SAI (Sistema de Altas IMSS)
sai_url: 'https://ppsai-abasto.imss.gob.mx/abasto-web/reporteAltas'
sai_user: 'tu_usuario_sai'
sai_password: 'tu_contraseña_sai'

# Credenciales PREI (Plataforma de Reportes)
prei_url: 'https://pispdigital.imss.gob.mx/piref/'
prei_user: 'tu_usuario_prei'  
prei_password: 'tu_contraseña_prei'

# Base de datos PostgreSQL (opcional)
sql_url: 'postgresql://usuario:contraseña@host:puerto/database'

# Configuración de columnas esperadas
columns_imss_altas: [lista_de_columnas]
columns_prei: [lista_de_columnas]
```

## Uso

### Ejecución Interactiva
```bash
python main.py
```

**Opciones del menú:**
1. **Descargar altas** - Descarga datos SAI del período especificado
2. **Descargar PREI** - Descarga contrarecibos por rangos de fechas
3. **Cargar facturas** - Procesa archivos PAQ de facturación
4. **Integrar información** - Combina todos los datasets
5. **Actualizar SQL** - Carga datos integrados a PostgreSQL
6. **Ejecutar consultas SQL** - Genera reportes predefinidos
7. **auto** - Ejecuta todo el proceso automáticamente

### Ejecución Automática Completa
Selecciona la opción `auto` para ejecutar todo el flujo:
1. Descarga altas SAI
2. Descarga contrarecibos PREI  
3. Procesa facturas
4. Integra toda la información
5. Actualiza base de datos
6. Genera reportes

## Estructura del Proyecto

```
mini_imss/
├── README.md
├── requirements.txt
├── main.py                          # 🎯 Orquestador principal
├── config.py                        # ⚙️ Gestor de configuración YAML
├── web_automation_driver.py         # 🤖 Manager de Selenium WebDriver
├── SAI.py                          # 📥 Módulo de descarga SAI/Altas
├── PREI.py                         # 📥 Módulo de descarga PREI
├── facturas_imss.py                # 💰 Procesador de facturas PAQ
├── downloaded_files_manager.py      # 📁 Organizador de archivos
├── data_integration.py             # 🔄 Integrador de datasets
├── sql_connexion_updating.py       # 🗄️ Conector PostgreSQL
├── helpers.py                      # 🛠️ Funciones auxiliares
├── sql_queries/                    # 📊 Consultas SQL predefinidas
│   └── strategy_status.sql
└── Implementación/                 # 📂 Carpeta generada automáticamente
    ├── config.yaml                 # ⚙️ Configuración del usuario
    ├── 2025_dates.xlsx            # 📅 Rangos de fechas para PREI
    ├── SAI/                       # 📁 Datos SAI
    │   ├── Temporal downloads/    # 📥 Descargas temporales
    │   └── SAI Altas_files/      # 📋 Altas procesadas
    ├── PREI/                      # 📁 Datos PREI
    │   ├── Temporal downloads/    # 📥 Descargas temporales
    │   └── PREI_files/           # 📋 Contrarecibos procesados
    ├── Facturas/                  # 📁 Datos de facturación
    │   └── Consultas/            # 📋 Archivos PAQ procesados
    └── Integración/               # 📁 Datasets combinados
        └── YYYY-MM-DD Integracion.xlsx
```

## Flujo de Datos

```mermaid
graph TD
    A[SAI - Altas IMSS] --> D[Integración]
    B[PREI - Contrarecibos] --> D
    C[Facturas PAQ] --> D
    D --> E[PostgreSQL]
    E --> F[Reportes SQL]
```

## Requisitos del Sistema

- **Sistema Operativo**: Windows 10/11, macOS (Apple Silicon/Intel)
- **Navegador**: Chrome se instala automáticamente
- **Conexión**: Internet estable para descargas IMSS
- **Permisos**: Acceso autorizado a sistemas IMSS/PREI
- **Hardware**: 4GB RAM mínimo, 2GB espacio en disco

## Solución de Problemas

### Errores Comunes
- **CAPTCHA**: El sistema pausa para validación manual en SAI
- **Archivos faltantes**: Verificar credenciales y permisos de descarga
- **Chrome no encontrado**: El sistema instala automáticamente la versión de testing
- **SQL Error**: Verificar cadena de conexión en config.yaml

### Logs y Depuración
El sistema muestra mensajes detallados en consola para seguimiento del proceso.

## Contribución

1. Fork del repositorio
2. Crear rama de feature (`git checkout -b feature/nueva-funcionalidad`)
3. Commit de cambios (`git commit -am 'Agregar nueva funcionalidad'`)
4. Push a la rama (`git push origin feature/nueva-funcionalidad`)
5. Crear Pull Request


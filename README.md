“Este repositorio muestra la evolución de un pipeline ETL en Python y SQLite, comenzando con una carga básica y evolucionando hacia un proceso batch con auditoría, idempotencia y modelo relacional.”

# ETL Pipeline en Python con SQLite

Este repositorio muestra la evolución de un **pipeline ETL (Extract, Transform, Load)** desarrollado en Python, utilizando **SQLite** como base de datos local.  
El proyecto comienza con una carga básica y evoluciona hasta un **ETL batch con auditoría, idempotencia y modelo relacional**.

El objetivo es demostrar fundamentos sólidos de **data engineering a nivel junior**, con buenas prácticas de estructura, validación y trazabilidad.

---

## Alcance del proyecto

El pipeline permite:

- Leer datos crudos desde archivos CSV
- Limpiar y normalizar información
- Rechazar registros inválidos con motivo
- Cargar datos en SQLite evitando duplicados
- Mantener auditoría de ejecuciones
- Procesar múltiples archivos en modo batch

---

## Estructura del repositorio

etl-python-sqlite/
│
├── src/
│ ├── etl_basic.py # ETL básico 
│ ├── etl_refactor.py # Refactorización a funciones 
│ ├── etl_from_csv.py # ETL leyendo desde CSV 
│ ├── etl_relational.py # Modelo relacional + rechazados 
│ ├── etl_incremental_audit.py # ETL incremental con auditoría 
│ └── etl_batch.py # ETL batch (múltiples CSV) 
│
├── data/
│ ├── in/ # Archivos CSV de entrada (ejemplo)
│ └── rejected/ # Registros rechazados (generados automáticamente)
│
├── .gitignore
└── README.md

---

## 🔄 Evolución del pipeline

### — ETL básico
- Limpieza de datos
- Carga a SQLite
- Prevención de duplicados

###  — Refactorización
- Separación en funciones `extract`, `transform`, `load`
- Código más mantenible

###  — Entrada desde CSV
- Lectura de datos externos
- Validaciones de formato

###  — Modelo relacional
- Tablas separadas (`personas`, `ciudades`)
- JOINs
- Manejo de registros rechazados (`rejected.csv`)

###  — Incremental + auditoría
- Carga incremental
- Campos `processed_at` y `run_id`
- Tabla `etl_runs` para trazabilidad

###  — ETL Batch
- Procesamiento de múltiples CSV
- Auditoría por archivo
- `run_id` único por ejecución
- Idempotencia total

---

##  Modelo de datos (SQLite)

### Tabla `personas_limpias`
- `persona_id`
- `nombre`
- `edad`
- `ciudad_id`
- `processed_at`
- `run_id`

### Tabla `ciudades`
- `ciudad_id`
- `nombre`

### Tabla `etl_runs`
- `run_id`
- `started_at`
- `source_file`
- `valid_count`
- `rejected_count`
- `inserted_new`
- `ignored_duplicates`

---

##  Cómo ejecutar el ETL batch

1. Coloca uno o más archivos `.csv` en:

data/in/

2. Ejecuta:

```bash
python src/etl_batch.py
Resultados:
Datos válidos cargados en SQLite
Rechazados guardados en data/rejected/
Auditoría registrada en etl_runs

Buenas prácticas implementadas
Idempotencia (el pipeline puede ejecutarse múltiples veces sin duplicar datos)
Validación y limpieza de datos
Separación de responsabilidades
Auditoría de ejecuciones
Manejo explícito de errores

 Tecnologías usadas
Python 3
SQLite
Librerías estándar (csv, sqlite3, pathlib, datetime)

```

Notas
Este proyecto es educativo y demostrativo, enfocado en mostrar el proceso y la evolución de un ETL realista, no en manejar grandes volúmenes de datos.

Autor
Guillermo MR
Ingeniero Físico | Aprendiendo Data Engineering

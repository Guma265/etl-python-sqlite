import sqlite3
from typing import List, Dict, Tuple


# =========================
# EXTRACT
# =========================
def extract() -> List[Dict[str, str]]:
    """Obtiene datos crudos (en producción vendría de CSV/API/etc.)."""
    return [
        {"nombre": "  guillermo ", "edad": "26", "ciudad": "san luis"},
        {"nombre": "NOEMI", "edad": "52", "ciudad": "SAN LUIS"},
        {"nombre": "Naomi ", "edad": "23", "ciudad": " san juan"},
        {"nombre": "Pedro", "edad": "error", "ciudad": "Querétaro"},
    ]


# =========================
# TRANSFORM
# =========================
def transform(datos_crudos: List[Dict[str, str]], edad_min: int = 25) -> List[Tuple[str, int, str]]:
    """
    Limpia, normaliza, convierte tipos y filtra por regla de negocio.
    Devuelve lista de tuplas lista para executemany: (nombre, edad, ciudad)
    """
    datos_limpios: List[Tuple[str, int, str]] = []

    for persona in datos_crudos:
        try:
            nombre = persona["nombre"].strip().lower().capitalize()
            edad = int(persona["edad"])
            ciudad = persona["ciudad"].strip().lower().title()

            if edad >= edad_min:
                datos_limpios.append((nombre, edad, ciudad))

        except (KeyError, ValueError, TypeError):
            # KeyError: falta una clave
            # ValueError: edad no convertible
            # TypeError: valor None/inesperado
            continue

    return datos_limpios


# =========================
# LOAD
# =========================
def ensure_table_with_unique(cursor: sqlite3.Cursor) -> None:
    """
    Garantiza que exista la tabla personas_limpias con UNIQUE(nombre, edad, ciudad).
    Si ya existía una versión sin UNIQUE, migra datos a una nueva tabla y renombra.
    """
    # Crear tabla nueva con UNIQUE
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS personas_limpias_new (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        nombre TEXT,
        edad INTEGER,
        ciudad TEXT,
        UNIQUE(nombre, edad, ciudad)
    )
    """)

    # Copiar datos desde tabla vieja si existe (sin duplicar)
    cursor.execute("""
    INSERT OR IGNORE INTO personas_limpias_new (nombre, edad, ciudad)
    SELECT nombre, edad, ciudad
    FROM personas_limpias
    """)

    # Reemplazar tabla vieja por la nueva
    cursor.execute("DROP TABLE IF EXISTS personas_limpias")
    cursor.execute("ALTER TABLE personas_limpias_new RENAME TO personas_limpias")


def load(db_path: str, datos_limpios: List[Tuple[str, int, str]]) -> None:
    """Carga datos limpios en SQLite evitando duplicados e imprime logs + validación."""
    if not datos_limpios:
        print("No hay datos limpios para cargar. (Transform devolvió lista vacía)")
        return

    conexion = sqlite3.connect(db_path)
    cursor = conexion.cursor()

    # Asegurar tabla con UNIQUE + migración si aplica
    ensure_table_with_unique(cursor)

    # Logs antes
    cursor.execute("SELECT COUNT(*) FROM personas_limpias")
    antes = cursor.fetchone()[0]
    intentados = len(datos_limpios)

    # Insertar sin duplicar
    cursor.executemany(
        "INSERT OR IGNORE INTO personas_limpias (nombre, edad, ciudad) VALUES (?, ?, ?)",
        datos_limpios
    )

    conexion.commit()

    # Logs después
    cursor.execute("SELECT COUNT(*) FROM personas_limpias")
    despues = cursor.fetchone()[0]

    insertados = despues - antes
    ignorados = intentados - insertados

    print("\nDatos cargados en SQLite (sin duplicados).")
    print("\n--- LOG ETL ---")
    print(f"Registros limpios (transform): {intentados}")
    print(f"Filas en tabla antes: {antes}")
    print(f"Insertados nuevos: {insertados}")
    print(f"Ignorados por duplicado: {ignorados}")
    print(f"Filas en tabla después: {despues}")

    # Validación
    cursor.execute("SELECT id, nombre, edad, ciudad FROM personas_limpias ORDER BY id")
    print("\nContenido final de personas_limpias:")
    for fila in cursor.fetchall():
        print(fila)

    conexion.close()


# =========================
# MAIN
# =========================
def main() -> None:
    db_path = "datos_etl.db"

    # EXTRACT
    datos_crudos = extract()

    # TRANSFORM
    datos_limpios = transform(datos_crudos, edad_min=25)

    print("Datos limpios:")
    for fila in datos_limpios:
        print(fila)

    # LOAD
    load(db_path, datos_limpios)


if __name__ == "__main__":
    main()

import csv
import sqlite3
from typing import List, Dict, Tuple


# =========================
# EXTRACT (desde CSV)
# =========================
def extract_csv(csv_path: str) -> List[Dict[str, str]]:
    with open(csv_path, mode="r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        return list(reader)


# =========================
# TRANSFORM
# =========================
def transform(datos_crudos: List[Dict[str, str]], edad_min: int = 25) -> List[Tuple[str, int, str]]:
    datos_limpios: List[Tuple[str, int, str]] = []

    for persona in datos_crudos:
        try:
            # Tomar valores (si falta una columna, KeyError)
            nombre_raw = persona["nombre"]
            edad_raw = persona["edad"]
            ciudad_raw = persona["ciudad"]

            # Normalizar
            nombre = nombre_raw.strip().lower().capitalize()
            edad = int(edad_raw)
            ciudad = ciudad_raw.strip().lower().title()

            # Regla de negocio
            if edad >= edad_min:
                datos_limpios.append((nombre, edad, ciudad))

        except (KeyError, ValueError, TypeError):
            # KeyError: faltan columnas
            # ValueError: edad no convertible
            # TypeError: None inesperado
            continue

    return datos_limpios


# =========================
# LOAD (SQLite sin duplicados)
# =========================
def ensure_table_with_unique(cursor: sqlite3.Cursor) -> None:
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS personas_limpias_new (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        nombre TEXT,
        edad INTEGER,
        ciudad TEXT,
        UNIQUE(nombre, edad, ciudad)
    )
    """)

    cursor.execute("""
    INSERT OR IGNORE INTO personas_limpias_new (nombre, edad, ciudad)
    SELECT nombre, edad, ciudad
    FROM personas_limpias
    """)

    cursor.execute("DROP TABLE IF EXISTS personas_limpias")
    cursor.execute("ALTER TABLE personas_limpias_new RENAME TO personas_limpias")


def load(db_path: str, datos_limpios: List[Tuple[str, int, str]]) -> None:
    if not datos_limpios:
        print("No hay datos limpios para cargar.")
        return

    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    ensure_table_with_unique(cur)

    # Logs antes
    cur.execute("SELECT COUNT(*) FROM personas_limpias")
    antes = cur.fetchone()[0]
    intentados = len(datos_limpios)

    # Insertar sin duplicar
    cur.executemany(
        "INSERT OR IGNORE INTO personas_limpias (nombre, edad, ciudad) VALUES (?, ?, ?)",
        datos_limpios
    )
    conn.commit()

    # Logs después
    cur.execute("SELECT COUNT(*) FROM personas_limpias")
    despues = cur.fetchone()[0]

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
    cur.execute("SELECT id, nombre, edad, ciudad FROM personas_limpias ORDER BY id")
    print("\nContenido final de personas_limpias:")
    for fila in cur.fetchall():
        print(fila)

    conn.close()


# =========================
# MAIN
# =========================
def main() -> None:
    csv_path = "personas_crudas.csv"
    db_path = "datos_etl.db"
    edad_min = 25

    # Extract
    datos_crudos = extract_csv(csv_path)

    # Transform
    datos_limpios = transform(datos_crudos, edad_min=edad_min)

    print("Datos limpios (desde CSV):")
    for fila in datos_limpios:
        print(fila)

    # Load
    load(db_path, datos_limpios)


if __name__ == "__main__":
    main()

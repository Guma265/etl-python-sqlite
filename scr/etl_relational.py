import csv
import sqlite3
from typing import List, Dict, Tuple


# -------------------------
# EXTRACT
# -------------------------
def extract_csv(csv_path: str) -> List[Dict[str, str]]:
    with open(csv_path, mode="r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        return list(reader)


# -------------------------
# TRANSFORM (válidos + rechazados)
# -------------------------
def transform_with_rejections(
    datos_crudos: List[Dict[str, str]],
    edad_min: int = 25
) -> Tuple[List[Tuple[str, int, str]], List[Dict[str, str]]]:
    """
    Devuelve:
      - validos: [(nombre, edad, ciudad_normalizada), ...]
      - rechazados: [{"nombre":..., "edad":..., "ciudad":..., "motivo":...}, ...]
    """
    validos: List[Tuple[str, int, str]] = []
    rechazados: List[Dict[str, str]] = []

    required_cols = {"nombre", "edad", "ciudad"}

    for row in datos_crudos:
        # Validar columnas
        if not required_cols.issubset(row.keys()):
            rechazados.append({
                "nombre": row.get("nombre", ""),
                "edad": row.get("edad", ""),
                "ciudad": row.get("ciudad", ""),
                "motivo": "Faltan columnas requeridas"
            })
            continue

        nombre_raw = row.get("nombre")
        edad_raw = row.get("edad")
        ciudad_raw = row.get("ciudad")

        # Validar None
        if nombre_raw is None or edad_raw is None or ciudad_raw is None:
            rechazados.append({
                "nombre": nombre_raw if nombre_raw is not None else "",
                "edad": edad_raw if edad_raw is not None else "",
                "ciudad": ciudad_raw if ciudad_raw is not None else "",
                "motivo": "Valor None en campo requerido"
            })
            continue

        # Normalización de texto
        try:
            nombre = nombre_raw.strip().lower().capitalize()
            ciudad = ciudad_raw.strip().lower().title()
        except Exception:
            rechazados.append({
                "nombre": str(nombre_raw),
                "edad": str(edad_raw),
                "ciudad": str(ciudad_raw),
                "motivo": "Error al normalizar texto"
            })
            continue

        # Edad a int
        try:
            edad = int(edad_raw)
        except ValueError:
            rechazados.append({
                "nombre": nombre_raw,
                "edad": edad_raw,
                "ciudad": ciudad_raw,
                "motivo": "Edad no convertible a int"
            })
            continue

        # Regla de negocio
        if edad < edad_min:
            rechazados.append({
                "nombre": nombre_raw,
                "edad": edad_raw,
                "ciudad": ciudad_raw,
                "motivo": f"Edad < {edad_min}"
            })
            continue

        validos.append((nombre, edad, ciudad))

    return validos, rechazados


def write_rejected_csv(rejected_path: str, rechazados: List[Dict[str, str]]) -> None:
    columnas = ["nombre", "edad", "ciudad", "motivo"]
    with open(rejected_path, mode="w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=columnas)
        writer.writeheader()
        writer.writerows(rechazados)


# -------------------------
# LOAD (modelo relacional)
# -------------------------
def ensure_schema(cursor: sqlite3.Cursor) -> None:
    # Tabla dimensión: ciudades
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS ciudades (
        ciudad_id INTEGER PRIMARY KEY AUTOINCREMENT,
        nombre TEXT NOT NULL UNIQUE
    )
    """)

    # Tabla personas: referencia a ciudad_id
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS personas_limpias (
        persona_id INTEGER PRIMARY KEY AUTOINCREMENT,
        nombre TEXT NOT NULL,
        edad INTEGER NOT NULL,
        ciudad_id INTEGER NOT NULL,
        UNIQUE(nombre, edad, ciudad_id),
        FOREIGN KEY (ciudad_id) REFERENCES ciudades(ciudad_id)
    )
    """)


def get_or_create_city_id(cursor: sqlite3.Cursor, ciudad: str) -> int:
    # Inserta ciudad si no existe
    cursor.execute("INSERT OR IGNORE INTO ciudades (nombre) VALUES (?)", (ciudad,))
    # Obtiene id
    cursor.execute("SELECT ciudad_id FROM ciudades WHERE nombre = ?", (ciudad,))
    return cursor.fetchone()[0]


def load_relational(db_path: str, validos: List[Tuple[str, int, str]]) -> None:
    if not validos:
        print("No hay datos válidos para cargar.")
        return

    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    ensure_schema(cur)

    # LOGS antes
    cur.execute("SELECT COUNT(*) FROM personas_limpias")
    antes = cur.fetchone()[0]
    intentados = len(validos)

    # Insertar fila por fila para asignar ciudad_id
    for (nombre, edad, ciudad) in validos:
        ciudad_id = get_or_create_city_id(cur, ciudad)
        cur.execute(
            "INSERT OR IGNORE INTO personas_limpias (nombre, edad, ciudad_id) VALUES (?, ?, ?)",
            (nombre, edad, ciudad_id)
        )

    conn.commit()

    # LOGS después
    cur.execute("SELECT COUNT(*) FROM personas_limpias")
    despues = cur.fetchone()[0]

    insertados = despues - antes
    ignorados = intentados - insertados

    print("\nDatos cargados en SQLite (modelo relacional, sin duplicados).")
    print("\n--- LOG LOAD ---")
    print(f"Registros válidos (transform): {intentados}")
    print(f"Filas en personas_limpias antes: {antes}")
    print(f"Insertados nuevos: {insertados}")
    print(f"Ignorados por duplicado: {ignorados}")
    print(f"Filas en personas_limpias después: {despues}")

    # -------------------------
    # JOIN de validación
    # -------------------------
    print("\n--- JOIN (personas + ciudades) ---")
    cur.execute("""
    SELECT
        p.persona_id,
        p.nombre,
        p.edad,
        c.nombre AS ciudad
    FROM personas_limpias p
    JOIN ciudades c
      ON p.ciudad_id = c.ciudad_id
    ORDER BY p.persona_id
    """)
    for fila in cur.fetchall():
        print(fila)

    # También: conteo por ciudad
    print("\n--- Conteo por ciudad (SQL) ---")
    cur.execute("""
    SELECT c.nombre AS ciudad, COUNT(*) AS total_personas, AVG(p.edad) AS edad_promedio
    FROM personas_limpias p
    JOIN ciudades c ON p.ciudad_id = c.ciudad_id
    GROUP BY c.nombre
    ORDER BY total_personas DESC
    """)
    for fila in cur.fetchall():
        print(fila)

    conn.close()


# -------------------------
# MAIN
# -------------------------
def main() -> None:
    csv_path = "personas_crudas.csv"
    rejected_path = "rejected.csv"
    db_path = "datos_etl_relacional.db"
    edad_min = 25

    # Extract
    datos_crudos = extract_csv(csv_path)

    # Transform
    validos, rechazados = transform_with_rejections(datos_crudos, edad_min=edad_min)

    print("Válidos (listos para cargar):")
    for v in validos:
        print(v)

    # Guardar rechazados
    write_rejected_csv(rejected_path, rechazados)
    print(f"\nRechazados guardados en: {rejected_path}  (total={len(rechazados)})")

    # Load relacional + JOIN
    load_relational(db_path, validos)


if __name__ == "__main__":
    main()

import csv
import sqlite3
from datetime import datetime, timezone
from typing import List, Dict, Tuple


# -------------------------
# EXTRACT
# -------------------------
def extract_csv(csv_path: str) -> List[Dict[str, str]]:
    with open(csv_path, mode="r", encoding="utf-8") as f:
        return list(csv.DictReader(f))


# -------------------------
# TRANSFORM
# -------------------------
def transform_with_rejections(
    datos_crudos: List[Dict[str, str]],
    edad_min: int = 25
) -> Tuple[List[Tuple[str, int, str]], List[Dict[str, str]]]:
    validos: List[Tuple[str, int, str]] = []
    rechazados: List[Dict[str, str]] = []
    required_cols = {"nombre", "edad", "ciudad"}

    for row in datos_crudos:
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

        if nombre_raw is None or edad_raw is None or ciudad_raw is None:
            rechazados.append({
                "nombre": nombre_raw if nombre_raw is not None else "",
                "edad": edad_raw if edad_raw is not None else "",
                "ciudad": ciudad_raw if ciudad_raw is not None else "",
                "motivo": "Valor None en campo requerido"
            })
            continue

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


def write_rejected_csv(path: str, rechazados: List[Dict[str, str]]) -> None:
    cols = ["nombre", "edad", "ciudad", "motivo"]
    with open(path, mode="w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        w.writerows(rechazados)


# -------------------------
# LOAD (Incremental + auditoría + migración)
# -------------------------
def ensure_ciudades(cursor: sqlite3.Cursor) -> None:
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS ciudades (
        ciudad_id INTEGER PRIMARY KEY AUTOINCREMENT,
        nombre TEXT NOT NULL UNIQUE
    )
    """)


def table_has_column(cursor: sqlite3.Cursor, table: str, column: str) -> bool:
    cursor.execute(f"PRAGMA table_info({table})")
    cols = [row[1] for row in cursor.fetchall()]  # row[1] = name
    return column in cols


def migrate_personas_limpias_if_needed(cursor: sqlite3.Cursor) -> None:
    """
    Si personas_limpias existe sin processed_at/run_id, migra a una tabla nueva con esas columnas.
    """
    cursor.execute("""
    SELECT name FROM sqlite_master
    WHERE type='table' AND name='personas_limpias'
    """)
    exists = cursor.fetchone() is not None

    if not exists:
        return  # se creará después

    # Si ya tiene las columnas nuevas, no hacemos nada
    if table_has_column(cursor, "personas_limpias", "processed_at") and table_has_column(cursor, "personas_limpias", "run_id"):
        return

    # Crear nueva tabla con esquema actualizado
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS personas_limpias_new (
        persona_id INTEGER PRIMARY KEY AUTOINCREMENT,
        nombre TEXT NOT NULL,
        edad INTEGER NOT NULL,
        ciudad_id INTEGER NOT NULL,
        processed_at TEXT NOT NULL,
        run_id TEXT NOT NULL,
        UNIQUE(nombre, edad, ciudad_id),
        FOREIGN KEY (ciudad_id) REFERENCES ciudades(ciudad_id)
    )
    """)

    # Copiar datos existentes: processed_at/run_id con valores default
    default_processed_at = "1970-01-01T00:00:00Z"
    default_run_id = "MIGRATION"

    cursor.execute("""
    INSERT OR IGNORE INTO personas_limpias_new (persona_id, nombre, edad, ciudad_id, processed_at, run_id)
    SELECT persona_id, nombre, edad, ciudad_id, ?, ?
    FROM personas_limpias
    """, (default_processed_at, default_run_id))

    # Reemplazar tabla vieja
    cursor.execute("DROP TABLE personas_limpias")
    cursor.execute("ALTER TABLE personas_limpias_new RENAME TO personas_limpias")


def ensure_personas_limpias(cursor: sqlite3.Cursor) -> None:
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS personas_limpias (
        persona_id INTEGER PRIMARY KEY AUTOINCREMENT,
        nombre TEXT NOT NULL,
        edad INTEGER NOT NULL,
        ciudad_id INTEGER NOT NULL,
        processed_at TEXT NOT NULL,
        run_id TEXT NOT NULL,
        UNIQUE(nombre, edad, ciudad_id),
        FOREIGN KEY (ciudad_id) REFERENCES ciudades(ciudad_id)
    )
    """)


def ensure_etl_runs(cursor: sqlite3.Cursor) -> None:
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS etl_runs (
        run_id TEXT PRIMARY KEY,
        started_at TEXT NOT NULL,
        source_file TEXT NOT NULL,
        valid_count INTEGER NOT NULL,
        rejected_count INTEGER NOT NULL,
        inserted_new INTEGER NOT NULL,
        ignored_duplicates INTEGER NOT NULL
    )
    """)


def get_or_create_city_id(cursor: sqlite3.Cursor, ciudad: str) -> int:
    cursor.execute("INSERT OR IGNORE INTO ciudades (nombre) VALUES (?)", (ciudad,))
    cursor.execute("SELECT ciudad_id FROM ciudades WHERE nombre = ?", (ciudad,))
    return cursor.fetchone()[0]


def load_incremental(
    db_path: str,
    source_file: str,
    validos: List[Tuple[str, int, str]],
    rejected_count: int
) -> None:
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    # 1) Asegurar dimensión
    ensure_ciudades(cur)

    # 2) Migrar si hace falta (por tablas viejas del Día 24)
    migrate_personas_limpias_if_needed(cur)

    # 3) Asegurar tablas finales
    ensure_personas_limpias(cur)
    ensure_etl_runs(cur)

    # Auditoría de corrida
    started_at = datetime.now(timezone.utc).isoformat()
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    processed_at = datetime.now(timezone.utc).isoformat()

    # Conteo antes
    cur.execute("SELECT COUNT(*) FROM personas_limpias")
    before = cur.fetchone()[0]

    # Insert incremental
    for (nombre, edad, ciudad) in validos:
        ciudad_id = get_or_create_city_id(cur, ciudad)
        cur.execute(
            """
            INSERT OR IGNORE INTO personas_limpias
            (nombre, edad, ciudad_id, processed_at, run_id)
            VALUES (?, ?, ?, ?, ?)
            """,
            (nombre, edad, ciudad_id, processed_at, run_id)
        )

    conn.commit()

    # Conteo después
    cur.execute("SELECT COUNT(*) FROM personas_limpias")
    after = cur.fetchone()[0]

    inserted_new = after - before
    ignored_duplicates = len(validos) - inserted_new

    # Registrar corrida
    cur.execute(
        """
        INSERT INTO etl_runs
        (run_id, started_at, source_file, valid_count, rejected_count, inserted_new, ignored_duplicates)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (run_id, started_at, source_file, len(validos), rejected_count, inserted_new, ignored_duplicates)
    )
    conn.commit()

    print("\n✅ LOAD incremental completo")
    print("--- LOG RUN ---")
    print(f"run_id: {run_id}")
    print(f"started_at (UTC): {started_at}")
    print(f"source_file: {source_file}")
    print(f"validos: {len(validos)} | rechazados: {rejected_count}")
    print(f"insertados_nuevos: {inserted_new} | duplicados_ignorados: {ignored_duplicates}")
    print(f"filas antes: {before} | filas después: {after}")

    print("\n--- Preview filas de esta corrida (JOIN) ---")
    cur.execute("""
    SELECT p.persona_id, p.nombre, p.edad, c.nombre AS ciudad, p.processed_at, p.run_id
    FROM personas_limpias p
    JOIN ciudades c ON p.ciudad_id = c.ciudad_id
    WHERE p.run_id = ?
    ORDER BY p.persona_id
    """, (run_id,))
    for row in cur.fetchall():
        print(row)

    conn.close()


# -------------------------
# MAIN
# -------------------------
def main() -> None:
    csv_path = "personas_crudas.csv"
    rejected_path = "rejected.csv"
    db_path = "datos_etl_relacional.db"
    edad_min = 25

    datos_crudos = extract_csv(csv_path)
    validos, rechazados = transform_with_rejections(datos_crudos, edad_min=edad_min)

    write_rejected_csv(rejected_path, rechazados)
    print(f"📄 Rechazados guardados en: {rejected_path} (total={len(rechazados)})")

    print("\nVálidos:")
    for v in validos:
        print(v)

    load_incremental(
        db_path=db_path,
        source_file=csv_path,
        validos=validos,
        rejected_count=len(rechazados)
    )


if __name__ == "__main__":
    main()

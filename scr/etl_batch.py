import csv
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Dict, Tuple


# -------------------------
# CONFIG (SOLUCIÓN PRO DE RUTAS)
# -------------------------
BASE_DIR = Path(__file__).resolve().parent  # carpeta donde vive este .py
DATA_IN = BASE_DIR / "data" / "in"
DATA_REJECTED = BASE_DIR / "data" / "rejected"
DB_PATH = BASE_DIR / "datos_etl_relacional.db"
EDAD_MIN = 25

DATA_IN.mkdir(parents=True, exist_ok=True)
DATA_REJECTED.mkdir(parents=True, exist_ok=True)


# -------------------------
# RUN_ID ÚNICO (SOLUCIÓN RÁPIDA)
# -------------------------
def make_run_id(source_file: str) -> str:
    # Microsegundos + nombre de archivo sanitizado = UNIQUE incluso si corre en el mismo segundo
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    safe = "".join(ch if ch.isalnum() else "_" for ch in source_file)
    return f"{ts}_{safe}"


# -------------------------
# EXTRACT
# -------------------------
def extract_csv(csv_path: Path) -> List[Dict[str, str]]:
    with csv_path.open(mode="r", encoding="utf-8") as f:
        return list(csv.DictReader(f))


# -------------------------
# TRANSFORM (válidos + rechazados)
# -------------------------
def transform_with_rejections(
    datos_crudos: List[Dict[str, str]],
    edad_min: int
) -> Tuple[List[Tuple[str, int, str]], List[Dict[str, str]]]:
    validos: List[Tuple[str, int, str]] = []
    rechazados: List[Dict[str, str]] = []
    required = {"nombre", "edad", "ciudad"}

    for row in datos_crudos:
        if not required.issubset(row.keys()):
            rechazados.append({**row, "motivo": "Faltan columnas"})
            continue

        try:
            nombre = row["nombre"].strip().lower().capitalize()
            ciudad = row["ciudad"].strip().lower().title()
            edad = int(row["edad"])
        except Exception:
            rechazados.append({**row, "motivo": "Normalización o tipo inválido"})
            continue

        if edad < edad_min:
            rechazados.append({**row, "motivo": f"Edad < {edad_min}"})
            continue

        validos.append((nombre, edad, ciudad))

    return validos, rechazados


def write_rejected_csv(out_path: Path, rechazados: List[Dict[str, str]]) -> None:
    if not rechazados:
        return
    cols = sorted(set().union(*[r.keys() for r in rechazados]))
    with out_path.open(mode="w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        w.writerows(rechazados)


# -------------------------
# LOAD (relacional + idempotente)
# -------------------------
def ensure_schema(cur: sqlite3.Cursor) -> None:
    cur.execute("""
    CREATE TABLE IF NOT EXISTS ciudades (
        ciudad_id INTEGER PRIMARY KEY AUTOINCREMENT,
        nombre TEXT NOT NULL UNIQUE
    )
    """)
    cur.execute("""
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
    cur.execute("""
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


def get_or_create_city_id(cur: sqlite3.Cursor, ciudad: str) -> int:
    cur.execute("INSERT OR IGNORE INTO ciudades (nombre) VALUES (?)", (ciudad,))
    cur.execute("SELECT ciudad_id FROM ciudades WHERE nombre = ?", (ciudad,))
    return cur.fetchone()[0]


def load_batch(
    conn: sqlite3.Connection,
    source_file: str,
    validos: List[Tuple[str, int, str]],
    rejected_count: int
) -> None:
    cur = conn.cursor()
    ensure_schema(cur)

    run_id = make_run_id(source_file)  # ✅ FIX: único por archivo aunque se procese en el mismo segundo
    started_at = datetime.now(timezone.utc).isoformat()
    processed_at = started_at

    cur.execute("SELECT COUNT(*) FROM personas_limpias")
    before = cur.fetchone()[0]

    for (nombre, edad, ciudad) in validos:
        ciudad_id = get_or_create_city_id(cur, ciudad)
        cur.execute(
            """INSERT OR IGNORE INTO personas_limpias
               (nombre, edad, ciudad_id, processed_at, run_id)
               VALUES (?, ?, ?, ?, ?)""",
            (nombre, edad, ciudad_id, processed_at, run_id)
        )

    conn.commit()

    cur.execute("SELECT COUNT(*) FROM personas_limpias")
    after = cur.fetchone()[0]

    inserted_new = after - before
    ignored = len(validos) - inserted_new

    cur.execute(
        """INSERT INTO etl_runs
           (run_id, started_at, source_file, valid_count, rejected_count,
            inserted_new, ignored_duplicates)
           VALUES (?, ?, ?, ?, ?, ?, ?)""",
        (run_id, started_at, source_file, len(validos), rejected_count, inserted_new, ignored)
    )
    conn.commit()

    print(f"\n📦 {source_file}")
    print(f"  válidos={len(validos)} rechazados={rejected_count}")
    print(f"  insertados_nuevos={inserted_new} duplicados_ignorados={ignored}")
    print(f"  run_id={run_id}")


# -------------------------
# MAIN (procesa carpeta)
# -------------------------
def main() -> None:
    archivos = sorted(DATA_IN.glob("*.csv"))
    if not archivos:
        print(f"No hay CSVs en: {DATA_IN}")
        print("👉 Coloca tus archivos .csv dentro de esa carpeta y vuelve a correr.")
        return

    conn = sqlite3.connect(str(DB_PATH))

    for csv_file in archivos:
        datos = extract_csv(csv_file)
        validos, rechazados = transform_with_rejections(datos, EDAD_MIN)

        rejected_out = DATA_REJECTED / f"rejected_{csv_file.name}"
        write_rejected_csv(rejected_out, rechazados)

        load_batch(
            conn=conn,
            source_file=csv_file.name,
            validos=validos,
            rejected_count=len(rechazados)
        )

    conn.close()
    print("\n✅ Batch ETL finalizado")


if __name__ == "__main__":
    main()

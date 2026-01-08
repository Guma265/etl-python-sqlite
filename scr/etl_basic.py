import sqlite3

# =========================
# EXTRACT
# =========================
datos_crudos = [
    {"nombre": "  guillermo ", "edad": "26", "ciudad": "san luis"},
    {"nombre": "NOEMI", "edad": "52", "ciudad": "SAN LUIS"},
    {"nombre": "Naomi ", "edad": "23", "ciudad": " san juan"},
    {"nombre": "Pedro", "edad": "error", "ciudad": "Querétaro"},
]

# =========================
# TRANSFORM
# =========================
datos_limpios = []

for persona in datos_crudos:
    try:
        nombre = persona["nombre"].strip().lower().capitalize()
        edad = int(persona["edad"])
        ciudad = persona["ciudad"].strip().lower().title()

        if edad >= 25:
            datos_limpios.append((nombre, edad, ciudad))

    except ValueError:
        continue

print("Datos limpios:")
for fila in datos_limpios:
    print(fila)

# =========================
# LOAD (con anti-duplicados)
# =========================
conexion = sqlite3.connect("datos_etl.db")
cursor = conexion.cursor()

# 1) Crear tabla NUEVA con UNIQUE
cursor.execute("""
CREATE TABLE IF NOT EXISTS personas_limpias_new (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    nombre TEXT,
    edad INTEGER,
    ciudad TEXT,
    UNIQUE(nombre, edad, ciudad)
)
""")

# 2) Si ya existía la tabla vieja, copiamos lo que tenga a la nueva (sin duplicar)
cursor.execute("""
INSERT OR IGNORE INTO personas_limpias_new (nombre, edad, ciudad)
SELECT nombre, edad, ciudad
FROM personas_limpias
""")

# 3) Borrar tabla vieja y renombrar la nueva
cursor.execute("DROP TABLE IF EXISTS personas_limpias")
cursor.execute("ALTER TABLE personas_limpias_new RENAME TO personas_limpias")

# 4) Insertar datos nuevos SIN duplicar
cursor.executemany(
    "INSERT OR IGNORE INTO personas_limpias (nombre, edad, ciudad) VALUES (?, ?, ?)",
    datos_limpios
)

conexion.commit()

print("Datos cargados en SQLite (sin duplicados).")

# =========================
# VALIDACIÓN (opcional, recomendado)
# =========================
cursor.execute("SELECT id, nombre, edad, ciudad FROM personas_limpias ORDER BY id")
print("\nContenido final de personas_limpias:")
for fila in cursor.fetchall():
    print(fila)

conexion.close()

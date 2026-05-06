"""
Prepara los datos de cultivos desde el Excel maestro y actualiza la DB local.
Lee 'Consolidado Riego 2026-2027.xlsx' hoja 'E Riego', extrae especie/variedad
por sector, normaliza, crea tabla `cultivos`, asigna `cultivo_id` en `sectores`.
"""

import sqlite3
import pandas as pd
import os

DB_PATH = "riego.db"
EXCEL_PATH = os.path.expanduser(r"~\OneDrive - auraoiliveoil.com\Escritorio\Trabajo\Consolidado Riego 2026 -2027 .xlsx")

SUPABASE_CULTIVOS = [
    (1, 'Olivo', 'Arbequina'),
    (2, 'Olivo', 'Arbosana'),
    (3, 'Cerezo', 'Bing'),
    (4, 'Avellano', 'Giffoni'),
    (5, 'Kiwi', None),
]

VARIEDAD_CORRECTIONS = {
    'pacific red': 'Pacific Red',
    'pacific red ': 'Pacific Red',
    'pacifi red': 'Pacific Red',
    'sweet aryana': 'Sweet Aryana',
    'sweet aryana ': 'Sweet Aryana',
    'korinenki': 'Korinenki',
    'arbosana': 'Arbosana',
    'arbequina': 'Arbequina',
    'avellano': 'Giffoni',
    'avellanos': 'Giffoni',
    'santina': 'Santina',
    'lapins': 'Lapins',
}

def key_of(text):
    if pd.isna(text) or not text:
        return None
    return str(text).strip().lower().replace('cereza', 'cerezo').replace('cerezas', 'cerezo')

def display_especie(text):
    if pd.isna(text) or not text:
        return None
    t = str(text).strip()
    t_lower = t.lower()
    if t_lower == 'cereza':
        return 'Cerezo'
    if t_lower == 'cerezas':
        return 'Cerezo'
    return t[0].upper() + t[1:] if t else t

def display_variedad(text):
    if pd.isna(text) or not text:
        return None
    t = str(text).strip()
    t_lower = t.lower().replace('cereza', 'cerezo').replace('cerezas', 'cerezo')
    if t_lower in VARIEDAD_CORRECTIONS:
        return VARIEDAD_CORRECTIONS[t_lower]
    return t

def main():
    print("=" * 60)
    print("  PREPARAR CULTIVOS")
    print("  Leyendo Excel y actualizando base de datos local")
    print("=" * 60)

    xl = pd.read_excel(EXCEL_PATH, sheet_name='E Riego')
    print(f"\nExcel leido: {len(xl)} filas en hoja 'E Riego'")

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    equipos = pd.read_sql("SELECT id, numero, nombre FROM equipos", conn)
    sectores = pd.read_sql("SELECT id, equipo_id, numero, nombre FROM sectores", conn)
    print(f"DB actual: {len(equipos)} equipos, {len(sectores)} sectores")

    eq_num_to_id = dict(zip(equipos['numero'], equipos['id']))

    # --- Paso 1: Construir lista completa de cultivos ---
    seen = {}
    cultivos_list = []

    for supa_id, nombre, variedad in SUPABASE_CULTIVOS:
        key = (key_of(nombre), key_of(variedad))
        seen[key] = supa_id
        cultivos_list.append({'id': supa_id, 'nombre': nombre, 'variedad': variedad})

    next_id = max(s[0] for s in SUPABASE_CULTIVOS) + 1
    for _, row in xl.iterrows():
        esp_key = key_of(row['Especie'])
        var_display = display_variedad(row['Variedad'])
        var_key = key_of(var_display)
        key = (esp_key, var_key)
        if key not in seen:
            seen[key] = next_id
            esp_display = display_especie(row['Especie'])
            cultivos_list.append({'id': next_id, 'nombre': esp_display, 'variedad': var_display})
            next_id += 1

    print(f"\nTotal cultivos únicos: {len(cultivos_list)}")

    # --- Paso 2: Crear/actualizar tabla cultivos ---
    cursor.execute("DROP TABLE IF EXISTS cultivos")
    cursor.execute("""
        CREATE TABLE cultivos (
            id INTEGER PRIMARY KEY,
            nombre TEXT NOT NULL,
            variedad TEXT
        )
    """)
    for c in cultivos_list:
        cursor.execute("INSERT INTO cultivos (id, nombre, variedad) VALUES (?, ?, ?)",
                       (c['id'], c['nombre'], c['variedad']))
    conn.commit()
    print("Tabla 'cultivos' creada con:")
    for c in cultivos_list:
        suf = f" - {c['variedad']}" if c['variedad'] else ''
        print(f"  {c['id']}: {c['nombre']}{suf}")

    # --- Paso 3: Columna cultivo_id en sectores ---
    cursor.execute("PRAGMA table_info(sectores)")
    cols = [col[1] for col in cursor.fetchall()]
    if 'cultivo_id' not in cols:
        cursor.execute("ALTER TABLE sectores ADD COLUMN cultivo_id INTEGER REFERENCES cultivos(id)")
        print("\nColumna 'cultivo_id' agregada a sectores")

    # --- Paso 4: Mapear Excel -> DB ---
    nivel_key = {}
    for _, row in xl.iterrows():
        eq = int(row['Equipo'])
        sc = int(row['Sector'])
        k = (key_of(row['Especie']), key_of(display_variedad(row['Variedad'])))
        cid = seen.get(k)
        if cid:
            nivel_key[(eq, sc)] = cid

    matched = 0
    unmatched = []
    for _, sec in sectores.iterrows():
        eq_nums = equipos[equipos['id'] == sec['equipo_id']]['numero'].values
        if len(eq_nums) == 0:
            continue
        eq = int(eq_nums[0])
        cid = nivel_key.get((eq, sec['numero']))
        cursor.execute("UPDATE sectores SET cultivo_id = ? WHERE id = ?", (cid, sec['id']))
        if cid:
            matched += 1
        else:
            unmatched.append(f"E{eq}S{sec['numero']}")

    conn.commit()
    print(f"\nSectores actualizados: {matched} con cultivo asignado")
    if unmatched:
        print(f"Sectores SIN cultivo: {', '.join(unmatched)}")

    # --- Paso 5: Índices ---
    for q in [
        "CREATE INDEX IF NOT EXISTS idx_sectores_equipo ON sectores(equipo_id)",
        "CREATE INDEX IF NOT EXISTS idx_sectores_cultivo ON sectores(cultivo_id)",
        "CREATE INDEX IF NOT EXISTS idx_rs_fecha ON riegos_solicitados(fecha_solicitado)",
        "CREATE INDEX IF NOT EXISTS idx_re_fecha ON riegos_ejecutados(fecha_ejecutado)",
        "CREATE INDEX IF NOT EXISTS idx_rs_equipo_sector ON riegos_solicitados(equipo_id, sector_id)",
        "CREATE INDEX IF NOT EXISTS idx_re_equipo_sector ON riegos_ejecutados(equipo_id, sector_id)",
    ]:
        cursor.execute(q)
    conn.commit()
    print("Índices creados")

    # --- Verificación ---
    verif = pd.read_sql("""
        SELECT s.nombre, c.nombre as especie, c.variedad
        FROM sectores s
        LEFT JOIN cultivos c ON s.cultivo_id = c.id
        ORDER BY s.id
    """, conn)
    print(f"\nVerificación: {verif['especie'].notna().sum()}/{len(verif)} sectores con especie")

    conn.close()
    print("\n¡Listo!")


if __name__ == '__main__':
    main()

"""
Sync script: Downloads all data from Supabase and saves to local SQLite database.
Now supports cultivos table and cultivo_id in sectores.
After sync, runs preparar_cultivos.py if the Excel file is available.
"""

import sqlite3
import requests
import pandas as pd
import subprocess
import sys
import os
from datetime import datetime

SUPABASE_URL = "https://miikjrfqmmkzknyngwen.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Im1paWtqcmZxbW1remtueW5nd2VuIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzE0NTI0MjgsImV4cCI6MjA4NzAyODQyOH0.msx8WdFropfpx2f0ll7OlBd9zPlTgq9KffL1ED-fJeM"

HEADERS = {
    'apikey': SUPABASE_KEY,
    'Authorization': f'Bearer {SUPABASE_KEY}',
    'Content-Type': 'application/json'
}

DB_PATH = "riego.db"


def fetch_all(url, params=None):
    results = []
    offset = 0
    page_size = 1000
    params = params or {}
    
    while True:
        r = requests.get(url, headers=HEADERS, params={**params, 'offset': offset, 'limit': page_size})
        if r.status_code != 200:
            print(f"Error fetching {url}: {r.status_code}")
            break
        data = r.json()
        if not data:
            break
        results.extend(data)
        if len(data) < page_size:
            break
        offset += page_size
    return results


def create_tables(conn):
    cursor = conn.cursor()
    
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS equipos (
            id INTEGER PRIMARY KEY,
            numero INTEGER,
            nombre TEXT
        )
    """)
    
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS sectores (
            id INTEGER PRIMARY KEY,
            equipo_id INTEGER,
            numero INTEGER,
            nombre TEXT
        )
    """)
    
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS cultivos (
            id INTEGER PRIMARY KEY,
            nombre TEXT NOT NULL,
            variedad TEXT
        )
    """)
    
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS riegos_solicitados (
            id INTEGER PRIMARY KEY,
            equipo_id INTEGER,
            sector_id INTEGER,
            fecha_solicitado TEXT,
            m3_estimados REAL
        )
    """)
    
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS riegos_ejecutados (
            id INTEGER PRIMARY KEY,
            equipo_id INTEGER,
            sector_id INTEGER,
            fecha_ejecutado TEXT,
            m3_reales REAL
        )
    """)
    
    cursor.execute("PRAGMA table_info(sectores)")
    cols = [col[1] for col in cursor.fetchall()]
    if 'cultivo_id' not in cols:
        cursor.execute("ALTER TABLE sectores ADD COLUMN cultivo_id INTEGER REFERENCES cultivos(id)")
    
    conn.commit()


def sync_equipos(conn):
    print("Descargando equipos...")
    data = fetch_all(f"{SUPABASE_URL}/rest/v1/equipos", {'select': 'id,numero,nombre'})
    cursor = conn.cursor()
    cursor.execute("DELETE FROM equipos")
    for e in data:
        cursor.execute("INSERT INTO equipos (id, numero, nombre) VALUES (?, ?, ?)",
            (e['id'], e['numero'], e.get('nombre')))
    conn.commit()
    print(f"  {len(data)} equipos guardados")


def sync_sectores(conn):
    print("Descargando sectores...")
    cursor = conn.cursor()
    cursor.execute("SELECT id, cultivo_id FROM sectores")
    old_mapping = {row[0]: row[1] for row in cursor.fetchall()}
    
    data = fetch_all(f"{SUPABASE_URL}/rest/v1/sectores", {'select': 'id,equipo_id,numero,nombre'})
    
    cursor.execute("DELETE FROM sectores")
    for s in data:
        cid = old_mapping.get(s['id'])
        cursor.execute("INSERT INTO sectores (id, equipo_id, numero, nombre, cultivo_id) VALUES (?, ?, ?, ?, ?)",
            (s['id'], s['equipo_id'], s['numero'], s.get('nombre'), cid))
    conn.commit()
    restored = sum(1 for s in data if old_mapping.get(s['id']))
    print(f"  {len(data)} sectores guardados ({restored} con cultivo_id preservado)")


def sync_cultivos(conn):
    print("Descargando cultivos...")
    data = fetch_all(f"{SUPABASE_URL}/rest/v1/cultivos", {'select': 'id,nombre,variedad'})
    if not data:
        print("  No hay cultivos en Supabase, saltando")
        return
    cursor = conn.cursor()
    cursor.execute("DELETE FROM cultivos")
    for c in data:
        cursor.execute("INSERT INTO cultivos (id, nombre, variedad) VALUES (?, ?, ?)",
            (c['id'], c['nombre'], c.get('variedad')))
    conn.commit()
    print(f"  {len(data)} cultivos guardados desde Supabase")


def sync_solicitados(conn):
    print("Descargando riegos_solicitados...")
    data = fetch_all(f"{SUPABASE_URL}/rest/v1/riegos_solicitados",
        {'select': 'id,equipo_id,sector_id,fecha_solicitado,m3_estimados'})
    cursor = conn.cursor()
    cursor.execute("DELETE FROM riegos_solicitados")
    for r in data:
        cursor.execute("INSERT INTO riegos_solicitados (id, equipo_id, sector_id, fecha_solicitado, m3_estimados) VALUES (?, ?, ?, ?, ?)",
            (r['id'], r['equipo_id'], r['sector_id'], r['fecha_solicitado'], r['m3_estimados']))
    conn.commit()
    print(f"  {len(data)} registros guardados")


def sync_ejecutados(conn):
    print("Descargando riegos_ejecutados...")
    data = fetch_all(f"{SUPABASE_URL}/rest/v1/riegos_ejecutados",
        {'select': 'id,equipo_id,sector_id,fecha_ejecutado,m3_reales'})
    cursor = conn.cursor()
    cursor.execute("DELETE FROM riegos_ejecutados")
    for r in data:
        cursor.execute("INSERT INTO riegos_ejecutados (id, equipo_id, sector_id, fecha_ejecutado, m3_reales) VALUES (?, ?, ?, ?, ?)",
            (r['id'], r['equipo_id'], r['sector_id'], r['fecha_ejecutado'], r['m3_reales']))
    conn.commit()
    print(f"  {len(data)} registros guardados")


def create_indexes(conn):
    cursor = conn.cursor()
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


def try_local_enrichment():
    excel_path = os.path.expanduser(r"~\OneDrive - auraoiliveoil.com\Escritorio\Trabajo\Consolidado Riego 2026 -2027 .xlsx")
    if os.path.exists(excel_path):
        print("\nExcel maestro encontrado. Ejecutando preparar_cultivos.py...")
        script_dir = os.path.dirname(os.path.abspath(__file__))
        prep_script = os.path.join(script_dir, "preparar_cultivos.py")
        if os.path.exists(prep_script):
            result = subprocess.run(
                [sys.executable, prep_script],
                capture_output=True, text=True, cwd=script_dir
            )
            print(result.stdout)
            if result.stderr:
                print(f"  WARN: {result.stderr}")
        else:
            print("  preparar_cultivos.py no encontrado, saltando")
    else:
        print("\nExcel maestro no encontrado. Los cultivos locales se cargan desde Supabase.")
        print("Ejecutá 'preparar_cultivos.py' manualmente si querés el mapeo completo.")


def main():
    print("=" * 60)
    print("  SYNC: Supabase -> SQLite local")
    print("=" * 60)
    print()
    
    conn = sqlite3.connect(DB_PATH)
    create_tables(conn)
    
    sync_equipos(conn)
    sync_cultivos(conn)
    sync_sectores(conn)
    sync_solicitados(conn)
    sync_ejecutados(conn)
    create_indexes(conn)
    
    conn.close()
    
    print()
    print("=" * 60)
    print(f"  Base de datos '{DB_PATH}' sincronizada exitosamente!")
    print("=" * 60)
    
    try_local_enrichment()


if __name__ == '__main__':
    main()

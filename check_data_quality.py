"""
Data Quality Check Script.
Identifies anomalies in the irrigation database:
- m3_reales == 0 in ejecutados
- m3_estimados outliers (> 2000)
- Sectors without species assigned
- Sectors with no irrigation records
"""

import sqlite3
import pandas as pd

DB_PATH = "riego.db"

print("=" * 70)
print("  DATA QUALITY CHECK - Analisis Riegos 2025-2026")
print("=" * 70)

conn = sqlite3.connect(DB_PATH)

equipos = pd.read_sql("SELECT id, numero, nombre FROM equipos", conn)
sectores = pd.read_sql("""
    SELECT s.id, s.nombre, e.numero as eq_num, s.numero as sec_num,
           c.nombre as especie, c.variedad
    FROM sectores s
    JOIN equipos e ON s.equipo_id = e.id
    LEFT JOIN cultivos c ON s.cultivo_id = c.id
    ORDER BY e.numero, s.numero
""", conn)
solicitados = pd.read_sql("SELECT * FROM riegos_solicitados", conn)
ejecutados = pd.read_sql("SELECT * FROM riegos_ejecutados", conn)

print(f"\n{'TOTALES':-^70}")
print(f"  Equipos:              {len(equipos)}")
print(f"  Sectores:             {len(sectores)}")
print(f"  Riegos solicitados:   {len(solicitados):,}")
print(f"  Riegos ejecutados:    {len(ejecutados):,}")

# ─── 1. m3_reales == 0 ───
ceros = ejecutados[ejecutados['m3_reales'] == 0]
print(f"\n{'1. EJECUTADOS CON m3_reales = 0':-^70}")
print(f"  Registros: {len(ceros)} ({(len(ceros)/len(ejecutados)*100):.1f}% del total)")
if len(ceros) > 0:
    ceros_detail = ceros.merge(
        sectores[['id', 'nombre', 'eq_num', 'sec_num', 'especie']],
        left_on='sector_id', right_on='id', how='left'
    )
    print(f"  Por especie:")
    for esp, cnt in ceros_detail['especie'].fillna('Sin asignar').value_counts().items():
        print(f"    {esp}: {cnt}")
    print(f"\n  Rango fechas: {ceros['fecha_ejecutado'].min()} a {ceros['fecha_ejecutado'].max()}")
    print(f"  Muestras (primeras 10):")
    for _, r in ceros_detail.head(10).iterrows():
        sector_label = f"E{r['eq_num']}S{r['sec_num']}" if pd.notna(r.get('eq_num')) else f"Sector {r['sector_id']}"
        esp = r['especie'] if pd.notna(r.get('especie')) else '?'
        print(f"    {r['fecha_ejecutado']} | {sector_label} ({esp}) | 0 m3")

# ─── 2. Outliers m3_estimados > 2000 ───
outliers = solicitados[solicitados['m3_estimados'] > 2000]
print(f"\n{'2. SOLICITADOS OUTLIERS (m3 > 2000)':-^70}")
print(f"  Registros: {len(outliers)}")
if len(outliers) > 0:
    out_detail = outliers.merge(
        sectores[['id', 'nombre', 'eq_num', 'sec_num', 'especie']],
        left_on='sector_id', right_on='id', how='left'
    )
    print(f"  Rango: {outliers['m3_estimados'].min():.0f} - {outliers['m3_estimados'].max():.0f} m3")
    print(f"  Por especie:")
    for esp, cnt in out_detail['especie'].fillna('Sin asignar').value_counts().items():
        print(f"    {esp}: {cnt}")
    print(f"\n  Top 10:")
    for _, r in out_detail.sort_values('m3_estimados', ascending=False).head(10).iterrows():
        sector_label = f"E{r['eq_num']}S{r['sec_num']}" if pd.notna(r.get('eq_num')) else f"Sector {r['sector_id']}"
        esp = r['especie'] if pd.notna(r.get('especie')) else '?'
        print(f"    {r['fecha_solicitado']} | {sector_label} ({esp}) | {r['m3_estimados']:.0f} m3")

# ─── 3. Outliers m3_reales > 2000 ───
out_ejec = ejecutados[ejecutados['m3_reales'] > 2000]
print(f"\n{'3. EJECUTADOS OUTLIERS (m3 > 2000)':-^70}")
print(f"  Registros: {len(out_ejec)}")
if len(out_ejec) > 0:
    oute_detail = out_ejec.merge(
        sectores[['id', 'nombre', 'eq_num', 'sec_num', 'especie']],
        left_on='sector_id', right_on='id', how='left'
    )
    print(f"  Rango: {out_ejec['m3_reales'].min():.0f} - {out_ejec['m3_reales'].max():.0f} m3")
    print(f"  Top 5:")
    for _, r in oute_detail.sort_values('m3_reales', ascending=False).head(5).iterrows():
        sector_label = f"E{r['eq_num']}S{r['sec_num']}" if pd.notna(r.get('eq_num')) else f"Sector {r['sector_id']}"
        esp = r['especie'] if pd.notna(r.get('especie')) else '?'
        print(f"    {r['fecha_ejecutado']} | {sector_label} ({esp}) | {r['m3_reales']:.0f} m3")

# ─── 4. Sectores sin especie ───
sin_esp = sectores[sectores['especie'].isna()]
print(f"\n{'4. SECTORES SIN ESPECIE ASIGNADA':-^70}")
print(f"  Cantidad: {len(sin_esp)}")
for _, r in sin_esp.iterrows():
    has_sol = len(solicitados[solicitados['sector_id'] == r['id']]) > 0
    has_ejec = len(ejecutados[ejecutados['sector_id'] == r['id']]) > 0
    status = "CON DATOS" if has_sol or has_ejec else "SIN DATOS"
    print(f"    E{r['eq_num']}S{r['sec_num']} ({status})")

# ─── 5. Sectores sin ningun registro ───
print(f"\n{'5. SECTORES SIN NINGUN REGISTRO':-^70}")
sin_reg = []
for _, r in sectores.iterrows():
    has_sol = len(solicitados[solicitados['sector_id'] == r['id']]) > 0
    has_ejec = len(ejecutados[ejecutados['sector_id'] == r['id']]) > 0
    if not has_sol and not has_ejec:
        sin_reg.append(r)
print(f"  Cantidad: {len(sin_reg)}")
for _, r in sectores.iterrows():
    has_sol = len(solicitados[solicitados['sector_id'] == r['id']]) > 0
    has_ejec = len(ejecutados[ejecutados['sector_id'] == r['id']]) > 0
    if not has_sol and not has_ejec:
        esp = r['especie'] if pd.notna(r.get('especie')) else 'Sin asignar'
        print(f"    E{r['eq_num']}S{r['sec_num']} ({esp})")

# ─── 6. Diferencia solicitudes vs ejecuciones ───
print(f"\n{'6. SOLICITADO vs EJECUTADO':-^70}")
total_sol = solicitados['m3_estimados'].sum()
total_ejec = ejecutados['m3_reales'].sum()
diff = total_ejec - total_sol
print(f"  Total solicitado:  {total_sol:,.0f} m3")
print(f"  Total ejecutado:   {total_ejec:,.0f} m3")
print(f"  Diferencia:        {diff:,.0f} m3 ({diff/total_sol*100:+.1f}%)")

# Solicitudes sin ejecucion
ids_sol = set(solicitados['sector_id'].unique())
ids_ejec = set(ejecutados['sector_id'].unique())
only_sol = ids_sol - ids_ejec
only_ejec = ids_ejec - ids_sol
print(f"  Sectores SOLO con solicitudes: {len(only_sol)}")
print(f"  Sectores SOLO con ejecuciones: {len(only_ejec)}")

conn.close()
print("\n" + "=" * 70)
print("  DATA QUALITY CHECK COMPLETADO")
print("=" * 70)

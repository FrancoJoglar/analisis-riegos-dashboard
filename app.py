"""
Irrigation Dashboard - Standalone version (no Supabase required)
Reads data from local SQLite database.
Supports species (cultivos) filter and advanced metrics.
"""

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import sqlite3
from datetime import datetime
import io

st.set_page_config(page_title="Analisis Riegos 2025-2026", layout="wide")

DB_PATH = "riego.db"


@st.cache_data(ttl=None)
def load_all_data():
    conn = sqlite3.connect(DB_PATH)

    equipos = pd.read_sql_query("SELECT id, numero, nombre FROM equipos", conn)
    sectores = pd.read_sql_query(
        """SELECT s.id, s.equipo_id, s.numero, s.nombre,
                  c.nombre as especie, c.variedad
           FROM sectores s
           LEFT JOIN cultivos c ON s.cultivo_id = c.id""", conn)
    cultivos = pd.read_sql_query("SELECT id, nombre, variedad FROM cultivos", conn)
    solicitados = pd.read_sql_query("SELECT * FROM riegos_solicitados", conn)
    ejecutados = pd.read_sql_query("SELECT * FROM riegos_ejecutados", conn)

    conn.close()

    eq_map = {e['id']: {'numero': e['numero'], 'nombre': e.get('nombre', f"E{e['numero']}")} for _, e in equipos.iterrows()}
    sec_map = {}
    for _, s in sectores.iterrows():
        sec_map[(s['equipo_id'], s['id'])] = {
            'numero': s['numero'],
            'nombre': s.get('nombre', f"S{s['numero']}"),
            'especie': s.get('especie') or 'Sin asignar',
            'variedad': s.get('variedad') or '',
        }

    df_sol = solicitados.copy()
    df_ejec = ejecutados.copy()

    df_sol['fecha'] = pd.to_datetime(df_sol['fecha_solicitado'])
    df_ejec['fecha'] = pd.to_datetime(df_ejec['fecha_ejecutado'])

    df_sol['equipo_num'] = df_sol['equipo_id'].map(lambda x: eq_map.get(x, {}).get('numero'))
    df_ejec['equipo_num'] = df_ejec['equipo_id'].map(lambda x: eq_map.get(x, {}).get('numero'))
    df_sol['equipo_nom'] = df_sol['equipo_id'].map(lambda x: eq_map.get(x, {}).get('nombre', f"E{eq_map.get(x, {}).get('numero', '?')}"))
    df_ejec['equipo_nom'] = df_ejec['equipo_id'].map(lambda x: eq_map.get(x, {}).get('nombre', f"E{eq_map.get(x, {}).get('numero', '?')}"))

    df_sol['sector_num'] = df_sol.apply(lambda r: sec_map.get((r['equipo_id'], r['sector_id']), {}).get('numero', r['sector_id']), axis=1)
    df_ejec['sector_num'] = df_ejec.apply(lambda r: sec_map.get((r['equipo_id'], r['sector_id']), {}).get('numero', r['sector_id']), axis=1)

    df_sol['sector_nom'] = df_sol.apply(lambda r: f"{eq_map.get(r['equipo_id'], {}).get('nombre', '?')} S{sec_map.get((r['equipo_id'], r['sector_id']), {}).get('numero', '?')}", axis=1)
    df_ejec['sector_nom'] = df_ejec.apply(lambda r: f"{eq_map.get(r['equipo_id'], {}).get('nombre', '?')} S{sec_map.get((r['equipo_id'], r['sector_id']), {}).get('numero', '?')}", axis=1)

    df_sol['especie'] = df_sol.apply(lambda r: sec_map.get((r['equipo_id'], r['sector_id']), {}).get('especie', 'Sin asignar'), axis=1)
    df_ejec['especie'] = df_ejec.apply(lambda r: sec_map.get((r['equipo_id'], r['sector_id']), {}).get('especie', 'Sin asignar'), axis=1)
    df_sol['variedad'] = df_sol.apply(lambda r: sec_map.get((r['equipo_id'], r['sector_id']), {}).get('variedad', ''), axis=1)
    df_ejec['variedad'] = df_ejec.apply(lambda r: sec_map.get((r['equipo_id'], r['sector_id']), {}).get('variedad', ''), axis=1)

    df_sol['especie_full'] = df_sol.apply(lambda r: f"{r['especie']} - {r['variedad']}" if r['variedad'] else r['especie'], axis=1)
    df_ejec['especie_full'] = df_ejec.apply(lambda r: f"{r['especie']} - {r['variedad']}" if r['variedad'] else r['especie'], axis=1)

    df_sol['mes'] = df_sol['fecha'].dt.to_period('M').astype(str)
    df_ejec['mes'] = df_ejec['fecha'].dt.to_period('M').astype(str)
    df_sol['semana'] = df_sol['fecha'].dt.isocalendar().week.astype(int)
    df_ejec['semana'] = df_ejec['fecha'].dt.isocalendar().week.astype(int)

    return df_sol, df_ejec, eq_map, sec_map, cultivos


# ── LOAD ──────────────────────────────────────────────────────────────────────

st.title("Analisis Riegos 2025-2026")
st.markdown("**Temporada: Octubre 2025 - Abril 2026**")

try:
    df_sol, df_ejec, eq_map, sec_map, cultivos = load_all_data()
except Exception as e:
    st.error(f"Error cargando datos: {e}")
    st.stop()

df_sol['tipo'] = 'Solicitado'
df_ejec['tipo'] = 'Ejecutado'
df_sol['m3'] = df_sol['m3_estimados']
df_ejec['m3'] = df_ejec['m3_reales'].fillna(0)


# ── SIDEBAR ───────────────────────────────────────────────────────────────────

st.sidebar.header("Filtros")

date_min = datetime(2025, 10, 1).date()
date_max = datetime(2026, 4, 30).date()

col1, col2 = st.sidebar.columns(2)
with col1:
    date_from = st.date_input("Desde", value=date_min, min_value=date_min, max_value=date_max)
with col2:
    date_to = st.date_input("Hasta", value=date_max, min_value=date_min, max_value=date_max)

all_equipos = sorted(df_sol['equipo_num'].dropna().unique().tolist())
equipos_selected = st.sidebar.multiselect("Equipos", all_equipos, default=all_equipos)

all_sectors = sorted(df_sol['sector_nom'].unique().tolist())
sectores_selected = st.sidebar.multiselect("Sectores", all_sectors, default=[])

all_especies = sorted(df_sol['especie'].dropna().unique().tolist())
especies_selected = st.sidebar.multiselect("Especie", all_especies, default=all_especies)


# ── FILTERS ───────────────────────────────────────────────────────────────────

mask_sol = (
    (df_sol['fecha'].dt.date >= date_from) &
    (df_sol['fecha'].dt.date <= date_to) &
    (df_sol['equipo_num'].isin(equipos_selected) if equipos_selected else True) &
    (df_sol['sector_nom'].isin(sectores_selected) if sectores_selected else True) &
    (df_sol['especie'].isin(especies_selected) if especies_selected else True)
)

mask_ejec = (
    (df_ejec['fecha'].dt.date >= date_from) &
    (df_ejec['fecha'].dt.date <= date_to) &
    (df_ejec['equipo_num'].isin(equipos_selected) if equipos_selected else True) &
    (df_ejec['sector_nom'].isin(sectores_selected) if sectores_selected else True) &
    (df_ejec['especie'].isin(especies_selected) if especies_selected else True)
)

df_s_f = df_sol[mask_sol].copy()
df_e_f = df_ejec[mask_ejec].copy()


# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 1: KPIs
# ═══════════════════════════════════════════════════════════════════════════════

total_sol = df_s_f['m3'].sum()
total_ejec = df_e_f['m3'].sum()
cumplimiento = (total_ejec / total_sol * 100) if total_sol > 0 else 0
diferencia = total_ejec - total_sol

kpi1, kpi2, kpi3, kpi4 = st.columns(4)
kpi1.metric("Solicitado (m3)", f"{total_sol:,.0f}")
kpi2.metric("Ejecutado (m3)", f"{total_ejec:,.0f}")
kpi3.metric("Cumplimiento", f"{cumplimiento:.1f}%", delta=f"{diferencia:,.0f} m3")
kpi4.metric("Solicitudes", f"{len(df_s_f):,}", delta=f"Ejec: {len(df_e_f):,}")

st.markdown("---")


# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 2: COMPARATIVA PRINCIPAL
# ═══════════════════════════════════════════════════════════════════════════════

st.subheader("Comparativa Solicitado vs Ejecutado")
group_by = st.radio("Agrupar por", ["Mes", "Equipo", "Sector", "Especie"], horizontal=True)

group_map = {
    "Mes": ('mes', 'Mes'),
    "Equipo": ('equipo_nom', 'Equipo'),
    "Sector": ('sector_nom', 'Sector'),
    "Especie": ('especie', 'Especie'),
}
group_col, group_label = group_map.get(group_by, ('mes', 'Mes'))

sol_agg = df_s_f.groupby(group_col)['m3'].sum().reset_index()
sol_agg.columns = [group_col, 'Solicitado']
ejec_agg = df_e_f.groupby(group_col)['m3'].sum().reset_index()
ejec_agg.columns = [group_col, 'Ejecutado']

combined = pd.merge(sol_agg, ejec_agg, on=group_col, how='outer').fillna(0)
if group_by == "Mes":
    combined = combined.sort_values(group_col)
elif group_by == "Equipo":
    combined['_sort'] = combined[group_col].str.extract(r'E?(\d+)').astype(float)
    combined = combined.sort_values('_sort')
elif group_by == "Especie":
    combined = combined.sort_values('Solicitado', ascending=False)
else:
    combined = combined.sort_values('Solicitado', ascending=False)

fmt = ",.0f"

fig = make_subplots(rows=1, cols=1)
fig.add_trace(go.Bar(
    x=combined[group_col], y=combined['Solicitado'],
    name='Solicitado', marker_color='#3498db', offsetgroup=0,
    text=combined['Solicitado'], texttemplate=f'%{{text:{fmt}}}', textposition='outside',
    textfont=dict(size=12, family='Arial Black'),
    hovertemplate=f'<b>%{{x}}</b><br>Solicitado: %{{y:{fmt}}} m³<extra></extra>'
))
fig.add_trace(go.Bar(
    x=combined[group_col], y=combined['Ejecutado'],
    name='Ejecutado', marker_color='#27ae60', offsetgroup=1,
    text=combined['Ejecutado'], texttemplate=f'%{{text:{fmt}}}', textposition='outside',
    textfont=dict(size=12, family='Arial Black'),
    hovertemplate=f'<b>%{{x}}</b><br>Ejecutado: %{{y:{fmt}}} m³<extra></extra>'
))
fig.update_layout(
    title=dict(text=f"", font=dict(size=16, family='Arial Black')),
    barmode='group', height=450,
    legend=dict(yanchor="top", y=0.99, xanchor="right", x=0.99, font=dict(size=13)),
    template='plotly_white',
    yaxis=dict(tickformat=fmt, title=dict(text='m³', font=dict(size=14, family='Arial Black'))),
    xaxis=dict(tickfont=dict(size=11)),
)
st.plotly_chart(fig, width='stretch')

# Detail table
detailed = combined[[group_col, 'Solicitado', 'Ejecutado']].copy()
detailed['Diferencia'] = detailed['Ejecutado'] - detailed['Solicitado']
detailed['Cumplimiento %'] = (detailed['Ejecutado'] / detailed['Solicitado'] * 100).round(1)
detailed = detailed.sort_values('Solicitado', ascending=False)
detailed.columns = [group_label, 'Solicitado (m3)', 'Ejecutado (m3)', 'Diferencia (m3)', 'Cumplimiento %']

st.dataframe(detailed, width='stretch', hide_index=True)

st.markdown("---")


# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 3: ANALISIS (tabs)
# ═══════════════════════════════════════════════════════════════════════════════

st.subheader("Analisis")

tab_esp, tab_trend, tab_heat, tab_drill, tab_export = st.tabs(
    ["Especies", "Tendencia", "Heatmap", "Drill Down", "Exportar"]
)

# ── TAB: ESPECIES ─────────────────────────────────────────────────────────────

with tab_esp:
    esp_sol = df_s_f.groupby('especie')['m3'].sum().reset_index()
    esp_ejec = df_e_f.groupby('especie')['m3'].sum().reset_index()
    esp_merged = pd.merge(esp_sol, esp_ejec, on='especie', how='outer', suffixes=('_sol', '_ejec')).fillna(0)
    esp_merged.columns = ['Especie', 'Solicitado', 'Ejecutado']
    esp_merged['Diferencia'] = esp_merged['Ejecutado'] - esp_merged['Solicitado']
    esp_merged['Cumplimiento'] = (esp_merged['Ejecutado'] / esp_merged['Solicitado'] * 100).round(1)
    esp_merged['Solicitado'] = esp_merged['Solicitado'].apply(lambda x: f"{x:,.0f}")
    esp_merged['Ejecutado'] = esp_merged['Ejecutado'].apply(lambda x: f"{x:,.0f}")
    esp_merged['Diferencia'] = esp_merged['Diferencia'].apply(lambda x: f"{x:,.0f}")

    c1, c2 = st.columns([2, 1])
    with c1:
        st.dataframe(esp_merged, width='stretch', hide_index=True)
    with c2:
        esp_dist = df_s_f.groupby('especie').size().reset_index(name='count')
        fig_pie = go.Figure(data=[go.Pie(
            labels=esp_dist['especie'],
            values=esp_dist['count'],
            hole=0.4,
            textinfo='label+percent',
        )])
        fig_pie.update_layout(title="Distribucion de Solicitudes por Especie", height=300, margin=dict(t=40, b=0, l=0, r=0))
        st.plotly_chart(fig_pie, width='stretch')


# ── TAB: TENDENCIA ────────────────────────────────────────────────────────────

with tab_trend:
    trend_gran = st.radio("Granularidad", ["Semanal", "Mensual"], horizontal=True, key="trend_gran")
    if trend_gran == "Semanal":
        df_s_f2 = df_s_f.copy()
        df_e_f2 = df_e_f.copy()
        df_s_f2['periodo'] = df_s_f2['fecha'].dt.isocalendar().week.astype(int).apply(lambda w: f"Sem {w}")
        df_e_f2['periodo'] = df_e_f2['fecha'].dt.isocalendar().week.astype(int).apply(lambda w: f"Sem {w}")
        period_key = 'periodo'
    else:
        df_s_f2 = df_s_f.copy()
        df_e_f2 = df_e_f.copy()
        df_s_f2['periodo'] = df_s_f2['mes']
        df_e_f2['periodo'] = df_e_f2['mes']
        period_key = 'periodo'

    t_sol = df_s_f2.groupby(period_key)['m3'].sum().reset_index()
    t_sol.columns = [period_key, 'Solicitado']
    t_ejec = df_e_f2.groupby(period_key)['m3'].sum().reset_index()
    t_ejec.columns = [period_key, 'Ejecutado']
    t_merged = pd.merge(t_sol, t_ejec, on=period_key, how='outer').fillna(0).sort_values(period_key)

    fig_trend = go.Figure()
    fig_trend.add_trace(go.Scatter(
        x=t_merged[period_key], y=t_merged['Solicitado'],
        mode='lines+markers', name='Solicitado',
        line=dict(color='#3498db', width=3),
        hovertemplate='<b>%{x}</b><br>Solicitado: %{y:,.0f} m³<extra></extra>'
    ))
    fig_trend.add_trace(go.Scatter(
        x=t_merged[period_key], y=t_merged['Ejecutado'],
        mode='lines+markers', name='Ejecutado',
        line=dict(color='#27ae60', width=3),
        hovertemplate='<b>%{x}</b><br>Ejecutado: %{y:,.0f} m³<extra></extra>'
    ))
    fig_trend.update_layout(
        title=dict(text=f"Tendencia {trend_gran.lower()}", font=dict(size=15, family='Arial Black')),
        height=350, template='plotly_white', hovermode='x unified',
        yaxis=dict(tickformat=',.0f', title=dict(text='m³', font=dict(size=13))),
    )
    st.plotly_chart(fig_trend, width='stretch')


# ── TAB: HEATMAP ──────────────────────────────────────────────────────────────

with tab_heat:
    hm_sol = df_s_f.groupby(['equipo_nom', 'mes'])['m3'].sum().reset_index()
    hm_ejec = df_e_f.groupby(['equipo_nom', 'mes'])['m3'].sum().reset_index()
    hm = pd.merge(hm_sol, hm_ejec, on=['equipo_nom', 'mes'], how='outer', suffixes=('_sol', '_ejec')).fillna(0)
    hm['cumpl'] = (hm['m3_ejec'] / hm['m3_sol'] * 100).clip(0, 200).round(1)

    pivot = hm.pivot_table(index='equipo_nom', columns='mes', values='cumpl', aggfunc='first')
    pivot = pivot.fillna(0)

    if pivot.empty:
        st.info("Sin datos para el heatmap con los filtros actuales")
    else:
        fig_hm = go.Figure(data=go.Heatmap(
            z=pivot.values,
            x=list(pivot.columns),
            y=list(pivot.index),
            colorscale='RdYlGn',
            zmin=0, zmax=150,
            text=pivot.values.astype(int),
            texttemplate='%{text}%',
            hovertemplate='Equipo: %{y}<br>Mes: %{x}<br>Cumpl: %{z:.1f}%<extra></extra>',
        ))
        fig_hm.update_layout(title="Cumplimiento % por Equipo y Mes", height=400, template='plotly_white')
        st.plotly_chart(fig_hm, width='stretch')

    st.markdown("**Top/Bottom Sectores por Cumplimiento**")
    sec_sol = df_s_f.groupby('sector_nom').agg({'m3': 'sum', 'especie': 'first'}).rename(columns={'m3': 'Solicitado'})
    sec_ejec = df_e_f.groupby('sector_nom').agg({'m3': 'sum', 'especie': 'first'}).rename(columns={'m3': 'Ejecutado'})
    sec_comp = pd.merge(sec_sol, sec_ejec[['Ejecutado']], on='sector_nom', how='outer').fillna(0)
    sec_comp['Cumpl'] = (sec_comp['Ejecutado'] / sec_comp['Solicitado'] * 100).round(1)
    sec_comp = sec_comp[sec_comp['Solicitado'] > 0].sort_values('Cumpl', ascending=False).reset_index()

    c1_top, c2_top = st.columns(2)
    with c1_top:
        st.markdown("**Top 5 - Mejor Cumplimiento**")
        best = sec_comp.head(5)[['sector_nom', 'especie', 'Solicitado', 'Ejecutado', 'Cumpl']].copy()
        best.columns = ['Sector', 'Especie', 'Sol (m3)', 'Ejec (m3)', 'Cumpl %']
        best['Sol (m3)'] = best['Sol (m3)'].apply(lambda x: f"{x:,.0f}")
        best['Ejec (m3)'] = best['Ejec (m3)'].apply(lambda x: f"{x:,.0f}")
        st.dataframe(best, width='stretch', hide_index=True)
    with c2_top:
        st.markdown("**Bottom 5 - Peor Cumplimiento**")
        worst = sec_comp.tail(5).sort_values('Cumpl')[['sector_nom', 'especie', 'Solicitado', 'Ejecutado', 'Cumpl']].copy()
        worst.columns = ['Sector', 'Especie', 'Sol (m3)', 'Ejec (m3)', 'Cumpl %']
        worst['Sol (m3)'] = worst['Sol (m3)'].apply(lambda x: f"{x:,.0f}")
        worst['Ejec (m3)'] = worst['Ejec (m3)'].apply(lambda x: f"{x:,.0f}")
        st.dataframe(worst, width='stretch', hide_index=True)


# ── TAB: DRILL DOWN ───────────────────────────────────────────────────────────

with tab_drill:
    drill_col1, drill_col2 = st.columns([1, 2])

    with drill_col1:
        drill_equipo = st.selectbox("Equipo", ["Todos"] + sorted(all_equipos), key="drill_eq")
        drill_sector = "Todos"

        if drill_equipo != "Todos":
            sectoros_eq = sorted(df_s_f[df_s_f['equipo_num'] == drill_equipo]['sector_nom'].unique().tolist())
            drill_sector = st.selectbox("Sector", ["Todos"] + sectoros_eq, key="drill_sec")

    with drill_col2:
        if drill_equipo != "Todos":
            mask_d_sol = (df_s_f['equipo_num'] == drill_equipo)
            mask_d_ejec = (df_e_f['equipo_num'] == drill_equipo)
            if drill_sector != "Todos":
                mask_d_sol = mask_d_sol & (df_s_f['sector_nom'] == drill_sector)
                mask_d_ejec = mask_d_ejec & (df_e_f['sector_nom'] == drill_sector)

            df_d_sol = df_s_f[mask_d_sol]
            df_d_ejec = df_e_f[mask_d_ejec]

            sol_m = df_d_sol['m3'].sum()
            ejec_m = df_d_ejec['m3'].sum()
            cumplimiento_d = (ejec_m / sol_m * 100) if sol_m > 0 else 0

            if drill_sector != "Todos":
                drill_especie = df_d_sol['especie'].iloc[0] if len(df_d_sol) > 0 else ''
                drill_variedad = df_d_sol['variedad'].iloc[0] if len(df_d_sol) > 0 else ''
                drill_label = f"{drill_equipo} - {drill_sector}"
                if drill_especie:
                    drill_label += f" ({drill_especie}"
                    if drill_variedad:
                        drill_label += f" - {drill_variedad}"
                    drill_label += ")"
            else:
                drill_label = str(drill_equipo)

            c1, c2, c3 = st.columns(3)
            c1.metric(f"Solicitado ({drill_label})", f"{sol_m:,.0f} m3")
            c2.metric("Ejecutado", f"{ejec_m:,.0f} m3")
            c3.metric("Cumplimiento", f"{cumplimiento_d:.1f}%", delta=f"{ejec_m - sol_m:,.0f} m3")

            by_month = df_d_sol.groupby('mes')['m3'].sum().reset_index()
            by_month.columns = ['Mes', 'Solicitado']
            by_month_e = df_d_ejec.groupby('mes')['m3'].sum().reset_index()
            by_month_e.columns = ['Mes', 'Ejecutado']
            combined_month = pd.merge(by_month, by_month_e, on='Mes', how='outer').fillna(0).sort_values('Mes')

            fig2 = make_subplots(rows=1, cols=1)
            fig2.add_trace(go.Bar(
                x=combined_month['Mes'], y=combined_month['Solicitado'],
                name='Solicitado', marker_color='#3498db',
                text=combined_month['Solicitado'], texttemplate='%{text:,.0f}', textposition='outside',
                textfont=dict(size=11, family='Arial Black'),
                hovertemplate='<b>%{x}</b><br>Solicitado: %{y:,.0f} m³<extra></extra>'
            ))
            fig2.add_trace(go.Bar(
                x=combined_month['Mes'], y=combined_month['Ejecutado'],
                name='Ejecutado', marker_color='#27ae60',
                text=combined_month['Ejecutado'], texttemplate='%{text:,.0f}', textposition='outside',
                textfont=dict(size=11, family='Arial Black'),
                hovertemplate='<b>%{x}</b><br>Ejecutado: %{y:,.0f} m³<extra></extra>'
            ))
            fig2.update_layout(
                title=dict(text=f"Solicitado vs Ejecutado - {drill_label}", font=dict(size=14, family='Arial Black')),
                barmode='group', height=320, template='plotly_white',
                yaxis=dict(tickformat=',.0f', title=dict(text='m³', font=dict(size=12))),
            )
            st.plotly_chart(fig2, width='stretch')
        else:
            st.info("Selecciona un equipo para ver el drill down")


# ── TAB: EXPORTAR ─────────────────────────────────────────────────────────────

with tab_export:
    export_df = detailed.copy()
    if 'Especie' not in export_df.columns:
        export_df['Especie'] = ''
    for col in ['Solicitado (m3)', 'Ejecutado (m3)', 'Diferencia (m3)']:
        export_df[col] = export_df[col].apply(lambda x: f"{x:,.2f}" if isinstance(x, (int, float)) else x)

    csv = export_df.to_csv(index=False).encode('utf-8')

    exp1, exp2 = st.columns(2)
    with exp1:
        st.download_button("Descargar CSV", csv, "riegos_comparativa.csv", "text/csv", use_container_width=True)
    with exp2:
        buffer = io.BytesIO()
        with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
            export_df.to_excel(writer, index=False)
        buffer.seek(0)
        st.download_button("Descargar Excel", buffer.getvalue(), "riegos_comparativa.xlsx",
                           "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", use_container_width=True)

st.markdown("---")
st.caption("Datos capturados: Primavera 2025 - Otono 2026 | Fuente: Supabase (snapshot local)")

import sqlite3
from reportlab.lib.pagesizes import landscape, A4
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import cm
from reportlab.lib import colors
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer
from reportlab.lib.enums import TA_CENTER, TA_LEFT

conn = sqlite3.connect('riego.db')
cur = conn.cursor()

cur.execute('''
SELECT equipo, sector, fecha, m3
FROM (
    SELECT e.numero || ' - ' || e.nombre as equipo,
           s.numero || ' - ' || s.nombre as sector,
           rs.fecha_solicitado as fecha,
           rs.m3_estimados as m3,
           ROW_NUMBER() OVER (PARTITION BY rs.sector_id ORDER BY rs.m3_estimados DESC, rs.fecha_solicitado DESC) as rn
    FROM riegos_solicitados rs
    JOIN sectores s ON rs.sector_id = s.id
    JOIN equipos e ON rs.equipo_id = e.id
) ranked
WHERE rn = 1
ORDER BY equipo, sector
''')
sectores_data = cur.fetchall()

conn.close()

output_path = "C:/Users/Usuario/Desktop/Riego_Reporte_Volumen.pdf"
doc = SimpleDocTemplate(
    output_path,
    pagesize=landscape(A4),
    leftMargin=1.5*cm, rightMargin=1.5*cm,
    topMargin=1.5*cm, bottomMargin=1.5*cm
)

styles = getSampleStyleSheet()
title_style = ParagraphStyle('Title', parent=styles['Title'], fontSize=18, alignment=TA_CENTER)
subtitle_style = ParagraphStyle('Subtitle', parent=styles['Normal'], fontSize=12, alignment=TA_CENTER, textColor=colors.grey)
normal_style = styles['Normal']

story = []

story.append(Paragraph("Riego con mayor volumen solicitado por sector", title_style))
story.append(Paragraph("Agronic - Siracusa 2025-2026", subtitle_style))
story.append(Spacer(1, 0.5*cm))

table_data = [['Equipo', 'Sector', 'Fecha', 'm³ solicitados']] + [
    [row[0], row[1], row[2], f"{row[3]:,.2f}"] for row in sectores_data
]

col_widths = [5.5*cm, 5.5*cm, 4.5*cm, 5*cm]
t = Table(table_data, colWidths=col_widths, repeatRows=1)
t.setStyle(TableStyle([
    ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#2c3e50')),
    ('TEXTCOLOR', (0, 0), (-1, 0), colors.white),
    ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
    ('FONTSIZE', (0, 0), (-1, 0), 11),
    ('FONTSIZE', (0, 1), (-1, -1), 10),
    ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
    ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
    ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, colors.HexColor('#ecf0f1')]),
    ('GRID', (0, 0), (-1, -1), 0.5, colors.grey),
    ('TOPPADDING', (0, 0), (-1, -1), 5),
    ('BOTTOMPADDING', (0, 0), (-1, -1), 5),
]))

story.append(t)

doc.build(story)
print(f"PDF generado: {output_path}")
import io
from datetime import datetime, timezone, timedelta
from reportlab.lib.pagesizes import letter
from reportlab.lib import colors
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, HRFlowable
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle

IST = timezone(timedelta(hours=5, minutes=30))

def generate_incident_brief_pdf(event, audits) -> bytes:
    """Generates an official National Weather Incident Brief in PDF."""
    buffer = io.BytesIO()
    doc = SimpleDocTemplate(buffer, pagesize=letter, rightMargin=36, leftMargin=36, topMargin=36, bottomMargin=36)
    story = []
    styles = getSampleStyleSheet()

    title_style = ParagraphStyle(
        'DocTitle',
        parent=styles['Heading1'],
        fontSize=18,
        leading=22,
        textColor=colors.HexColor('#0f172a'),
        fontName='Helvetica-Bold'
    )
    subtitle_style = ParagraphStyle(
        'DocSubtitle',
        parent=styles['Normal'],
        fontSize=10,
        leading=14,
        textColor=colors.HexColor('#475569')
    )
    section_style = ParagraphStyle(
        'SectionHeader',
        parent=styles['Heading2'],
        fontSize=12,
        leading=16,
        textColor=colors.HexColor('#0369a1'),
        fontName='Helvetica-Bold'
    )
    body_style = ParagraphStyle(
        'Body',
        parent=styles['Normal'],
        fontSize=9,
        leading=13,
        textColor=colors.HexColor('#1e293b')
    )

    obs_str = str(event.observed_at)
    if isinstance(event.observed_at, datetime):
        obs_str = event.observed_at.strftime('%d %b %Y, %I:%M %p IST')
    elif obs_str and 'T' in obs_str:
        try:
            dt = datetime.fromisoformat(obs_str.replace('Z', '+00:00'))
            dt_ist = dt.astimezone(IST) if dt.tzinfo else dt + timedelta(hours=5, minutes=30)
            obs_str = dt_ist.strftime('%d %b %Y, %I:%M %p IST')
        except Exception:
            obs_str = f"{obs_str} IST"

    story.append(Paragraph("GOVERNMENT OF INDIA · NATIONAL DISASTER MANAGEMENT AUTHORITY", subtitle_style))
    story.append(Paragraph("OPERATIONAL WEATHER INCIDENT BRIEF", title_style))
    story.append(Paragraph(f"Reference ID: <b>{event.id}</b> · Generated at: {datetime.now(IST).strftime('%d %b %Y, %I:%M %p IST')}", subtitle_style))
    story.append(Spacer(1, 10))
    story.append(HRFlowable(width="100%", thickness=1.5, color=colors.HexColor('#0284c7'), spaceAfter=12))

    overview_data = [
        [Paragraph("<b>Incident Title</b>", body_style), Paragraph(str(event.title), body_style)],
        [Paragraph("<b>Category & Severity</b>", body_style), Paragraph(f"{event.category} · <b>{event.severity}</b>", body_style)],
        [Paragraph("<b>Location</b>", body_style), Paragraph(f"{event.city}, {event.state} (Lat: {event.latitude:.4f}, Lng: {event.longitude:.4f})", body_style)],
        [Paragraph("<b>Uber H3 Index</b>", body_style), Paragraph(f"<code>{event.h3_index}</code> (Resolution 7)", body_style)],
        [Paragraph("<b>Observed Timestamp</b>", body_style), Paragraph(obs_str, body_style)],
        [Paragraph("<b>Source & Provenance</b>", body_style), Paragraph(f"{event.source} (Author: {event.source_author or 'Anonymous'})", body_style)],
        [Paragraph("<b>Verification Status</b>", body_style), Paragraph(f"<b>{event.verification_status}</b> (TrustScore: {event.trust_score}%)", body_style)],
        [Paragraph("<b>Operator Decision</b>", body_style), Paragraph(f"<b>{event.operator_decision}</b>", body_style)]
    ]
    t1 = Table(overview_data, colWidths=[150, 390])
    t1.setStyle(TableStyle([
        ('BACKGROUND', (0,0), (-1,-1), colors.HexColor('#f8fafc')),
        ('GRID', (0,0), (-1,-1), 0.5, colors.HexColor('#cbd5e1')),
        ('PADDING', (0,0), (-1,-1), 5),
    ]))
    story.append(t1)
    story.append(Spacer(1, 14))

    story.append(Paragraph("Incident Field Description", section_style))
    story.append(Paragraph(event.description, body_style))
    story.append(Spacer(1, 14))

    story.append(Paragraph("AI Truth & Radar Ground-Truth Evidence", section_style))
    radar_info = f"Nearest IMD Station: <b>{event.radar_station_name or 'N/A'}</b> | Recorded Value: <b>{event.radar_recorded_value or 0.0} mm/kmh</b> | Radar Corroborated: <b>{'YES' if event.radar_corroborated else 'NO'}</b>"
    media_info = f"Media Authenticity: <b>{'AUTHENTIC' if event.is_media_authentic else 'RECYCLED/MANIPULATED FLAG'}</b> | Media Type: {event.media_type}"
    evidence_data = [
        [Paragraph("<b>Physical Station Check</b>", body_style), Paragraph(radar_info, body_style)],
        [Paragraph("<b>Multimodal Vision AI</b>", body_style), Paragraph(media_info, body_style)],
        [Paragraph("<b>NLP Semantic Confidence</b>", body_style), Paragraph(f"{event.nlp_confidence * 100:.1f}% factual consistency", body_style)]
    ]
    t2 = Table(evidence_data, colWidths=[150, 390])
    t2.setStyle(TableStyle([
        ('BACKGROUND', (0,0), (-1,-1), colors.HexColor('#f0fdf4')),
        ('GRID', (0,0), (-1,-1), 0.5, colors.HexColor('#86efac')),
        ('PADDING', (0,0), (-1,-1), 5),
    ]))
    story.append(t2)
    story.append(Spacer(1, 14))

    story.append(Paragraph("Immutable Cryptographic Audit Trail", section_style))
    audit_rows = [[
        Paragraph("<b>Timestamp (IST)</b>", body_style),
        Paragraph("<b>Operator / Engine</b>", body_style),
        Paragraph("<b>Action</b>", body_style),
        Paragraph("<b>Justification / Evidence</b>", body_style)
    ]]
    for a in audits:
        a_time = str(a.timestamp)
        if isinstance(a.timestamp, datetime):
            a_time = a.timestamp.strftime('%H:%M:%S IST')
        audit_rows.append([
            Paragraph(a_time, body_style),
            Paragraph(a.operator_name, body_style),
            Paragraph(a.action, body_style),
            Paragraph(a.reason[:120] + ('...' if len(a.reason) > 120 else ''), body_style)
        ])
    t3 = Table(audit_rows, colWidths=[90, 110, 100, 240])
    t3.setStyle(TableStyle([
        ('BACKGROUND', (0,0), (-1,0), colors.HexColor('#e2e8f0')),
        ('GRID', (0,0), (-1,-1), 0.5, colors.HexColor('#94a3b8')),
        ('PADDING', (0,0), (-1,-1), 4),
    ]))
    story.append(t3)

    doc.build(story)
    pdf_bytes = buffer.getvalue()
    buffer.close()
    return pdf_bytes

from routers.reports import generate_single_report, get_report_styles
import xhtml2pdf.pisa as pisa
import io

m = {
    'id': 1, 'date': '2023-01-01', 'time': '10:00', 'end_time': '11:00',
    'stichwort': 'Test', 'adresse': 'Test', 'meldung': 'Test',
    'duration': 1.0, 'status': 'Freigegeben', 'leader_signature': 'Test',
    'gname': 'Feuerwehr', 'instructors': 'Einsatzleiter', 'category': 'Einsatz',
    'description': 'Test'
}
persons = [{'name': 'Test', 'is_present': 1, 'vehicle': 'HLF', 'signature': None}]

html_content = generate_single_report(m, persons, 'Deine Feuerwehr')
full_html = f"<html><head><meta charset='utf-8'><style>{get_report_styles()}</style></head><body>{html_content}</body></html>"

pdf_buf = io.BytesIO()
pisa.CreatePDF(full_html, dest=pdf_buf)
print('PDF generated successfully')

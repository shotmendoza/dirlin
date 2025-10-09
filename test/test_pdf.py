from dirlin import Folder
from dirlin import PDFFile


f = Folder("/Volumes/USB321FD/Commission Statements")


def test_pdf_open():
    path = f / f"Connie LiuPeng Statement Commissions 2024.03.pdf"
    pdf = PDFFile(path)
    df = pdf.to_dataframe(skip_first_per_page=True, debug=True)
    print(df)


def test_problem_pdf():
    path = f / f"Connie LiuPeng Statement Commissions 2024.06.pdf"
    print(path)
    pdf = PDFFile(path)
    df = pdf.to_dataframe(skip_first_per_page=True, debug=True)
    print(df)

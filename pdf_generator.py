from re import template

import jinja2
from xhtml2pdf import pisa


class PdfGenerator:

    def __init__(self) -> None:
        
        templateLoader = jinja2.FileSystemLoader(searchpath="./template")
        templateEnv = jinja2.Environment(loader=templateLoader)
        TEMPLATE_FILE = "index.html"
        self.template = templateEnv.get_template(TEMPLATE_FILE)
        self.output_file_name = "render.pdf"

    # FIXME what to include in data?
    def generate_report_pdf(self, data):
        """Generate PDF"""
        html = self.__get_html(data)

        err = self.__convert_html_to_pdf(html)

        if err != 0:
            raise Exception(f'Failed to generate report')


    def __get_html(self, data):
        source_html = self.template.render(json_data=data)
        
        return source_html

    def __convert_html_to_pdf(self, sourceHtml):
        """Convert to PDF"""
        resultFile = open(self.output_file_name, "w+b")

        pisa_status = pisa.CreatePDF(src=sourceHtml, dest=resultFile)

        resultFile.close()

        print(pisa_status.err)
        
        return pisa_status.err

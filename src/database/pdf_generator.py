r"""
C:\Economy\Invest\TrendMaster\src\database\pdf_generator.py
PDF report generator for customer orders
python -m src.database.pdf_generator
"""

from pathlib import Path
from datetime import datetime
from typing import List, Dict
import logging

logger = logging.getLogger(__name__)

from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch
from reportlab.lib.enums import TA_CENTER

class CustomerOrderPDFGenerator:
    """Generate PDF reports for customer orders"""

    def __init__(self, output_dir: Path = None):
        """Initialize PDF generator

        Args:
            output_dir: Directory to save PDFs (defaults to data/print)
        """

        self.output_dir = output_dir or Path(__file__).parent.parent.parent / "data" / "print"
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def generate_customer_report(self, customer_data: Dict, orders_data: List[Dict]) -> str:
        """Generate PDF report for a customer

        Args:
            customer_data: Dictionary with customer info (customer_id, first_name, last_name, email, phone, street, city, state, zip_code)
            orders_data: List of order dictionaries (order_id, order_date, order_status, staff_id, store_id, items)

        Returns:
            str: Path to generated PDF file
        """
        # Create filename
        customer_name = f"{customer_data['first_name']}_{customer_data['last_name']}"
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"customer_report_{customer_data['customer_id']}_{customer_name}_{timestamp}.pdf"
        filepath = self.output_dir / filename

        # Create PDF document
        doc = SimpleDocTemplate(str(filepath), pagesize=A4,
                               rightMargin=72, leftMargin=72,
                               topMargin=72, bottomMargin=18)

        # Container for the 'Flowable' objects
        elements = []

        # Define styles
        styles = getSampleStyleSheet()
        title_style = ParagraphStyle(
            'CustomTitle',
            parent=styles['Heading1'],
            fontSize=24,
            textColor=colors.HexColor('#1976d2'),
            spaceAfter=30,
            alignment=TA_CENTER
        )

        heading_style = ParagraphStyle(
            'CustomHeading',
            parent=styles['Heading2'],
            fontSize=16,
            textColor=colors.HexColor('#1976d2'),
            spaceAfter=12,
            spaceBefore=12
        )

        # Title
        title = Paragraph("Customer Order Report", title_style)
        elements.append(title)
        elements.append(Spacer(1, 12))

        # Customer Information Section
        elements.append(Paragraph("Customer Information", heading_style))

        customer_info = [
            ['Customer ID:', str(customer_data['customer_id'])],
            ['Name:', f"{customer_data['first_name']} {customer_data['last_name']}"],
            ['Email:', customer_data.get('email', 'N/A')],
            ['Phone:', customer_data.get('phone', 'N/A')],
            ['Address:', f"{customer_data.get('street', 'N/A')}, {customer_data.get('city', 'N/A')}, {customer_data.get('state', 'N/A')} {customer_data.get('zip_code', 'N/A')}"]
        ]

        customer_table = Table(customer_info, colWidths=[2*inch, 4*inch])
        customer_table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (0, -1), colors.HexColor('#f0f0f0')),
            ('TEXTCOLOR', (0, 0), (-1, -1), colors.black),
            ('ALIGN', (0, 0), (0, -1), 'RIGHT'),
            ('ALIGN', (1, 0), (1, -1), 'LEFT'),
            ('FONTNAME', (0, 0), (0, -1), 'Helvetica-Bold'),
            ('FONTNAME', (1, 0), (1, -1), 'Helvetica'),
            ('FONTSIZE', (0, 0), (-1, -1), 10),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 8),
            ('TOPPADDING', (0, 0), (-1, -1), 8),
            ('GRID', (0, 0), (-1, -1), 1, colors.grey)
        ]))
        elements.append(customer_table)
        elements.append(Spacer(1, 24))

        # Orders Section
        elements.append(Paragraph(f"Order History ({len(orders_data)} orders)", heading_style))

        if orders_data:
            # Calculate totals
            total_orders = len(orders_data)
            # Handle None values in total_amount
            total_amount = sum(order.get('total_amount') or 0 for order in orders_data)

            # Summary
            summary_data = [
                ['Total Orders:', str(total_orders)],
                ['Total Amount:', f"{total_amount:,.2f} kr"]
            ]
            summary_table = Table(summary_data, colWidths=[2*inch, 2*inch])
            summary_table.setStyle(TableStyle([
                ('BACKGROUND', (0, 0), (0, -1), colors.HexColor('#e3f2fd')),
                ('TEXTCOLOR', (0, 0), (-1, -1), colors.black),
                ('ALIGN', (0, 0), (0, -1), 'RIGHT'),
                ('ALIGN', (1, 0), (1, -1), 'LEFT'),
                ('FONTNAME', (0, 0), (-1, -1), 'Helvetica-Bold'),
                ('FONTSIZE', (0, 0), (-1, -1), 10),
                ('BOTTOMPADDING', (0, 0), (-1, -1), 8),
                ('TOPPADDING', (0, 0), (-1, -1), 8),
                ('GRID', (0, 0), (-1, -1), 1, colors.grey)
            ]))
            elements.append(summary_table)
            elements.append(Spacer(1, 16))

            # Orders table
            order_table_data = [['Order ID', 'Date', 'Status', 'Items', 'Total']]

            for order in orders_data:
                order_table_data.append([
                    str(order.get('order_id', 'N/A')),
                    str(order.get('order_date', 'N/A')),
                    str(order.get('order_status', 'N/A')),
                    str(order.get('item_count') or 0),
                    f"{order.get('total_amount') or 0:,.2f} kr"
                ])

            orders_table = Table(order_table_data, colWidths=[1*inch, 1.5*inch, 1.2*inch, 0.8*inch, 1.5*inch])
            orders_table.setStyle(TableStyle([
                ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#1976d2')),
                ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
                ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
                ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                ('FONTSIZE', (0, 0), (-1, 0), 11),
                ('FONTNAME', (0, 1), (-1, -1), 'Helvetica'),
                ('FONTSIZE', (0, 1), (-1, -1), 9),
                ('BOTTOMPADDING', (0, 0), (-1, -1), 6),
                ('TOPPADDING', (0, 0), (-1, -1), 6),
                ('GRID', (0, 0), (-1, -1), 1, colors.grey),
                ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, colors.HexColor('#f5f5f5')])
            ]))
            elements.append(orders_table)
        else:
            elements.append(Paragraph("No orders found for this customer.", styles['Normal']))

        elements.append(Spacer(1, 24))

        # Footer
        footer_text = f"Report generated on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        footer = Paragraph(footer_text, styles['Italic'])
        elements.append(footer)

        # Build PDF
        doc.build(elements)

        logger.info(f"Generated customer report: {filepath}")
        return str(filepath)

# ------------------------------------------------------------------
# Local manual tests
# ------------------------------------------------------------------
import tempfile

if __name__ == "__main__":
    print("Running local CustomerOrderPDFGenerator tests...")

    # -----------------------------
    # Setup temp output directory
    # -----------------------------
    temp_dir = Path(tempfile.mkdtemp())
    print(f"Using temp dir: {temp_dir}")

    generator = CustomerOrderPDFGenerator(output_dir=temp_dir)

    # -----------------------------
    # Mock test data
    # -----------------------------
    customer_data = {
        "customer_id": 1,
        "first_name": "John",
        "last_name": "Doe",
        "email": "john@example.com",
        "phone": "123456789",
        "street": "Main St 1",
        "city": "Tel Aviv",
        "state": "IL",
        "zip_code": "12345",
    }

    orders_data = [
        {
            "order_id": 101,
            "order_date": "2024-01-01",
            "order_status": "Completed",
            "item_count": 3,
            "total_amount": 150.75,
        },
        {
            "order_id": 102,
            "order_date": "2024-02-01",
            "order_status": "Shipped",
            "item_count": 1,
            "total_amount": 50.00,
        },
    ]

    # -----------------------------
    # Test: generate PDF
    # -----------------------------
    try:
        pdf_path = generator.generate_customer_report(customer_data, orders_data)
        print("✅ PDF generated:", pdf_path)

        # Verify file exists
        path_obj = Path(pdf_path)
        assert path_obj.exists(), "PDF file was not created"
        assert path_obj.stat().st_size > 0, "PDF file is empty"

        print("✅ File exists and is non-empty")

    except Exception as e:
        print("❌ Failed to generate PDF:", e)

    # -----------------------------
    # Test: empty orders
    # -----------------------------
    try:
        pdf_path = generator.generate_customer_report(customer_data, [])
        print("✅ PDF generated with empty orders:", pdf_path)
    except Exception as e:
        print("❌ Failed on empty orders:", e)

    print("🎉 Local tests completed")
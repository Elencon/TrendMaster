import pytest
from pathlib import Path

# Correct: Since 'src' is in the pythonpath, start from the subfolder name
from src.database.pdf_generator import CustomerOrderPDFGenerator


# -----------------------------
# Fixtures
# -----------------------------
@pytest.fixture
def temp_output_dir(tmp_path):
    """Fixture to provide a temporary directory for PDF generation."""
    dir_path = tmp_path / "reports"
    dir_path.mkdir()  # Ensure directory exists
    return dir_path


@pytest.fixture
def sample_data():
    """Fixture providing valid customer and order data."""
    customer = {
        "customer_id": 101,
        "first_name": "Ilya",
        "last_name": "Shpilberg",
        "email": "ilya@example.com",
        "city": "Tel Aviv",
        "state": "IL"
    }
    orders = [
        {"order_id": 1, "order_date": "2026-03-01", "order_status": "Completed", "total_amount": 1500.50},
        {"order_id": 2, "order_date": "2026-03-15", "order_status": "Pending", "total_amount": 450.00}
    ]
    return customer, orders


# -----------------------------
# Test PDF generation normal case
# -----------------------------
def test_pdf_generation_creates_file(temp_output_dir, sample_data):
    """Verify that a PDF file is actually written to the disk."""
    customer, orders = sample_data
    generator = CustomerOrderPDFGenerator(output_dir=temp_output_dir)

    result_path = generator.generate_customer_report(customer, orders)

    result_file = Path(result_path)
    assert result_file.exists()
    assert result_path.endswith(".pdf")
    
    # Updated assertion to match actual filename prefix: customer_report_
    assert f"customer_report_{customer['customer_id']}" in result_path
    
    assert result_file.stat().st_size > 0  # Optional: file is not empty


# -----------------------------
# Test PDF generation with empty orders
# -----------------------------
def test_pdf_generation_empty_orders(temp_output_dir, sample_data):
    """Verify the generator handles a customer with no orders (edge case)."""
    customer, _ = sample_data
    generator = CustomerOrderPDFGenerator(output_dir=temp_output_dir)

    result_path = generator.generate_customer_report(customer, [])

    result_file = Path(result_path)
    assert result_file.exists()
    assert result_path.endswith(".pdf")
    assert result_file.stat().st_size > 0


# -----------------------------
# Test permission error fallback
# -----------------------------
def test_permission_error_fallback(temp_output_dir, sample_data, monkeypatch):
    customer, orders = sample_data
    generator = CustomerOrderPDFGenerator(output_dir=temp_output_dir)

    from reportlab.platypus import SimpleDocTemplate
    real_build = SimpleDocTemplate.build

    # We use a dict to track state across multiple calls safely
    state = {"call_count": 0}

    def side_effect_logic(*args, **kwargs):
        state["call_count"] += 1

        # Trigger the simulated failure on the first attempt
        if state["call_count"] == 1:
            raise PermissionError("File locked")

        # On the second attempt, call the original build method.
        return real_build(*args, **kwargs)

    # Patch the class method with our logic
    monkeypatch.setattr(SimpleDocTemplate, "build", side_effect_logic)

    # Run the generator
    result_path = generator.generate_customer_report(customer, orders)

    # Verify the fallback logic worked
    assert "_NEW.pdf" in result_path
    assert Path(result_path).exists()
    assert state["call_count"] == 2


# -----------------------------
# Test missing keys in input data
# -----------------------------
def test_missing_keys_in_data(temp_output_dir):
    """Verify robustness when input dictionaries are missing expected keys."""
    generator = CustomerOrderPDFGenerator(output_dir=temp_output_dir)

    # Minimal data with missing fields
    incomplete_customer = {"customer_id": "ERR"}
    incomplete_orders = [{"order_id": 999}]  # Missing total_amount and order_date

    result_path = generator.generate_customer_report(incomplete_customer, incomplete_orders)

    result_file = Path(result_path)
    assert result_file.exists()
    assert result_path.endswith(".pdf")
    assert result_file.stat().st_size > 0
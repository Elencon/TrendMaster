import pytest
import pandas as pd
import sys
from pathlib import Path

# Add src to python path for testing
src_path = Path("c:/Economy/Invest/TrendMaster/src")
if str(src_path) not in sys.path:
    sys.path.insert(0, str(src_path))

from database.data_validator import (
    DataValidator, 
    ValidationRule, 
    DataType,
    create_data_validator
)

def test_data_validator_init():
    validator = DataValidator()
    assert validator.rules == {}
    assert validator.stats["validated_rows"] == 0

def test_add_rule():
    validator = DataValidator()
    rule = ValidationRule(field_name="age", data_type=DataType.INTEGER, min_value=18)
    validator.add_rule(rule)
    assert "age" in validator.rules
    assert validator.rules["age"].min_value == 18

def test_validate_dataframe_empty():
    validator = DataValidator()
    empty_df = pd.DataFrame()
    result = validator.validate_dataframe(empty_df)
    
    assert result.is_valid is False
    assert len(result.issues) == 1
    assert result.issues[0]["type"] == "empty_dataframe"

def test_validate_dataframe_required_missing():
    validator = DataValidator()
    validator.add_rule(ValidationRule("email", DataType.STRING, required=True))
    
    df = pd.DataFrame({"name": ["Alice", "Bob"]})
    result = validator.validate_dataframe(df)
    
    assert result.is_valid is False
    assert any(issue["type"] == "missing_column" for issue in result.issues)

def test_validate_integer_type(sample_df):
    validator = DataValidator()
    validator.add_rule(ValidationRule("age", DataType.INTEGER))
    
    result = validator.validate_dataframe(sample_df)
    assert result.is_valid is True
    
    # Introduce bad data
    bad_df = sample_df.copy()
    bad_df.loc[0, "age"] = "not-a-number"
    
    bad_result = validator.validate_dataframe(bad_df)
    assert bad_result.is_valid is False
    assert any(issue["type"] == "invalid_integer" for issue in bad_result.issues)

def test_validate_email_type(sample_df):
    validator = DataValidator()
    validator.add_rule(ValidationRule("email", DataType.EMAIL))
    
    result = validator.validate_dataframe(sample_df)
    assert result.is_valid is False
    
    email_issue = next(i for i in result.issues if i["type"] == "invalid_email")
    assert email_issue["count"] == 1 # "bob@bad-email" is invalid

def test_clean_dataframe(sample_df):
    validator = DataValidator()
    # String truncation rule
    validator.add_rule(ValidationRule("name", DataType.STRING, max_length=3))
    
    cleaned_df = validator.clean_dataframe(sample_df)
    
    assert cleaned_df.loc[0, "name"] == "Ali" # "Alice" truncated to 3 chars
    assert cleaned_df.loc[1, "name"] == "Bob"

def test_create_common_rules():
    from database.data_validator import create_common_rules
    rules = create_common_rules()
    
    assert "customer_id" in rules
    assert "email" in rules
    assert rules["customer_id"].data_type == DataType.INTEGER

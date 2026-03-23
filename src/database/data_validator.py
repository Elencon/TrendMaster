"""
Compact data validation pipeline using utility-driven approach.
"""

import logging
import re
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Optional

import pandas as pd

logger = logging.getLogger(__name__)

# Severities that cause is_valid=False / has_errors=True
_BLOCKING_SEVERITIES: frozenset[str] = frozenset({"error", "critical"})


class ValidationSeverity(Enum):
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


class DataType(Enum):
    STRING = "string"
    INTEGER = "integer"
    FLOAT = "float"
    BOOLEAN = "boolean"
    DATE = "date"
    EMAIL = "email"
    CATEGORICAL = "categorical"


@dataclass
class ValidationRule:
    field_name: str
    data_type: DataType
    required: bool = True
    min_value: Optional[int | float] = None
    max_value: Optional[int | float] = None
    min_length: Optional[int] = None
    max_length: Optional[int] = None
    pattern: Optional[str] = None
    allowed_values: Optional[list] = None


@dataclass
class ValidationResult:
    is_valid: bool
    issues: list[dict] = field(default_factory=list)
    summary: dict = field(default_factory=dict)


# Pre-compiled for performance across large DataFrames
_EMAIL_RE = re.compile(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$')


def _is_blocking(issue: dict) -> bool:
    return issue["severity"] in _BLOCKING_SEVERITIES


class DataValidator:
    """Compact data validator."""

    def __init__(self) -> None:
        self.rules: dict[str, ValidationRule] = {}

    def add_rule(self, rule: ValidationRule) -> None:
        self.rules[rule.field_name] = rule

    def validate_dataframe(self, df: pd.DataFrame) -> ValidationResult:
        issues: list[dict] = []

        # Schema validation
        for col, rule in self.rules.items():
            if col not in df.columns and rule.required:
                issues.append({
                    "type": "missing_column",
                    "column": col,
                    "severity": ValidationSeverity.ERROR.value,
                    "message": f"Required column '{col}' is missing",
                })

        # Per-column validation
        for col_name, rule in self.rules.items():
            if col_name in df.columns:
                issues.extend(self._validate_column(df[col_name], rule))

        summary = self._create_summary(issues, len(df))
        return ValidationResult(
            is_valid=not any(_is_blocking(i) for i in issues),
            issues=issues,
            summary=summary,
        )

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _validate_column(self, series: pd.Series, rule: ValidationRule) -> list[dict]:
        issues: list[dict] = []
        col_name = rule.field_name

        null_count = int(series.isnull().sum())
        if null_count > 0 and rule.required:
            issues.append({
                "type": "null_values",
                "column": col_name,
                "count": null_count,
                "severity": ValidationSeverity.ERROR.value,
                "message": f"Column '{col_name}' has {null_count} null values",
            })

        valid_series = series.dropna()
        if not valid_series.empty:
            issues.extend(self._validate_data_type(valid_series, rule))

        return issues

    def _validate_data_type(self, series: pd.Series, rule: ValidationRule) -> list[dict]:
        issues: list[dict] = []
        col_name = rule.field_name

        if rule.data_type == DataType.INTEGER:
            def _is_int(x: Any) -> bool:
                try:
                    return float(x).is_integer()
                except (ValueError, TypeError):
                    return False

            non_int = ~series.apply(_is_int)
            if non_int.any():
                issues.append({
                    "type": "invalid_integer",
                    "column": col_name,
                    "count": int(non_int.sum()),
                    "severity": ValidationSeverity.ERROR.value,
                    "message": f"Column '{col_name}' has non-integer values",
                })

        elif rule.data_type == DataType.EMAIL:
            invalid_emails = ~series.astype(str).str.match(_EMAIL_RE, na=False)
            if invalid_emails.any():
                issues.append({
                    "type": "invalid_email",
                    "column": col_name,
                    "count": int(invalid_emails.sum()),
                    "severity": ValidationSeverity.ERROR.value,
                    "message": f"Column '{col_name}' has invalid email formats",
                })

        if rule.data_type == DataType.STRING:
            str_len = series.astype(str).str.len()
            if rule.min_length is not None:
                too_short = str_len < rule.min_length
                if too_short.any():
                    issues.append({
                        "type": "string_too_short",
                        "column": col_name,
                        "count": int(too_short.sum()),
                        "severity": ValidationSeverity.WARNING.value,
                        "message": f"Column '{col_name}' has values shorter than {rule.min_length}",
                    })
            if rule.max_length is not None:
                too_long = str_len > rule.max_length
                if too_long.any():
                    issues.append({
                        "type": "string_too_long",
                        "column": col_name,
                        "count": int(too_long.sum()),
                        "severity": ValidationSeverity.ERROR.value,
                        "message": f"Column '{col_name}' has values longer than {rule.max_length}",
                    })

        if rule.min_value is not None or rule.max_value is not None:
            numeric = pd.to_numeric(series, errors="coerce")
            if rule.min_value is not None:
                too_small = numeric < rule.min_value
                if too_small.any():
                    issues.append({
                        "type": "value_too_small",
                        "column": col_name,
                        "count": int(too_small.sum()),
                        "severity": ValidationSeverity.WARNING.value,
                        "message": f"Column '{col_name}' has values below {rule.min_value}",
                    })
            if rule.max_value is not None:
                too_large = numeric > rule.max_value
                if too_large.any():
                    issues.append({
                        "type": "value_too_large",
                        "column": col_name,
                        "count": int(too_large.sum()),
                        "severity": ValidationSeverity.WARNING.value,
                        "message": f"Column '{col_name}' has values above {rule.max_value}",
                    })

        if rule.allowed_values is not None:
            invalid = ~series.isin(rule.allowed_values)
            if invalid.any():
                issues.append({
                    "type": "invalid_categorical_value",
                    "column": col_name,
                    "count": int(invalid.sum()),
                    "severity": ValidationSeverity.ERROR.value,
                    "message": f"Column '{col_name}' has values not in allowed set",
                })

        return issues

    @staticmethod
    def _create_summary(issues: list[dict], total_rows: int) -> dict:
        severity_counts: dict[str, int] = {}
        for issue in issues:
            severity_counts[issue["severity"]] = severity_counts.get(issue["severity"], 0) + 1

        has_errors = any(_is_blocking(i) for i in issues)
        return {
            "total_issues": len(issues),
            "total_rows": total_rows,
            "severity_counts": severity_counts,
            "has_errors": has_errors,
            "validation_passed": not has_errors,
        }

    def clean_dataframe(self, df: pd.DataFrame, fix_issues: bool = True) -> pd.DataFrame:
        if not fix_issues:
            return df.copy()

        cleaned_df = df.copy()
        for col_name, rule in self.rules.items():
            if col_name in cleaned_df.columns:
                cleaned_df[col_name] = self._clean_column(cleaned_df[col_name], rule)
        return cleaned_df

    def _clean_column(self, series: pd.Series, rule: ValidationRule) -> pd.Series:
        cleaned = series.copy()
        if rule.data_type == DataType.STRING and rule.max_length:
            mask = cleaned.astype(str).str.len() > rule.max_length
            cleaned.loc[mask] = cleaned.loc[mask].astype(str).str[: rule.max_length]
        if rule.allowed_values is not None:
            cleaned.loc[~cleaned.isin(rule.allowed_values)] = None
        return cleaned


# ---------------------------------------------------------------------------
# Convenience helpers
# ---------------------------------------------------------------------------

def create_common_rules() -> dict[str, ValidationRule]:
    return {
        "customer_id": ValidationRule("customer_id", DataType.INTEGER, required=True, min_value=1),
        "order_id":    ValidationRule("order_id",    DataType.INTEGER, required=True, min_value=1),
        "product_id":  ValidationRule("product_id",  DataType.INTEGER, required=True, min_value=1),
        "email":       ValidationRule("email",       DataType.EMAIL,   required=False, max_length=255),
        "phone":       ValidationRule("phone",       DataType.STRING,  required=False, min_length=10, max_length=20),
        "first_name":  ValidationRule("first_name",  DataType.STRING,  required=True,  min_length=1, max_length=100),
        "last_name":   ValidationRule("last_name",   DataType.STRING,  required=True,  min_length=1, max_length=100),
    }


def validate_csv_file(
    file_path: str,
    rules: dict[str, ValidationRule],
    *,
    encoding: str = "utf-8",
) -> ValidationResult:
    try:
        df = pd.read_csv(file_path, encoding=encoding)
        validator = DataValidator()
        for rule in rules.values():
            validator.add_rule(rule)
        return validator.validate_dataframe(df)
    except Exception as e:
        logger.error("CSV validation failed: %s", e)
        return ValidationResult(
            is_valid=False,
            issues=[{
                "type": "file_error",
                "severity": ValidationSeverity.CRITICAL.value,
                "message": f"Failed to read/validate file: {e}",
            }],
        )


def create_data_validator() -> DataValidator:
    """Factory for backward compatibility."""
    return DataValidator()
"""
=============================================================
  VALIDATORS / DATA_VALIDATOR.PY  —  The Quality Inspector
=============================================================

WHAT IS DATA VALIDATION?
  After cleaning the data, we need to CHECK that it meets
  our quality standards before saving it.

  Like a food inspector at a restaurant:
    ✅ No expired ingredients
    ✅ Temperature is correct
    ✅ No forbidden additives

  We check things like:
    ✅ Required columns exist
    ✅ Numbers are within valid ranges (age: 0–120)
    ✅ Email addresses look like emails
    ✅ No more than X% of rows are empty
    ✅ IDs are unique (no duplicates)

WHAT HAPPENS IF VALIDATION FAILS?
  We have two options:
    "strict"  — STOP everything (refuse to load bad data)
    "warn"    — log a warning but continue anyway

  The choice depends on how important accuracy is for your use case.
=============================================================
"""

import re
from dataclasses import dataclass, field
from typing import List, Optional, Callable, Any
from enum import Enum
import pandas as pd
from loguru import logger


# ── ENUMS ─────────────────────────────────────────────────
class Severity(Enum):
    """
    How serious is a rule violation?
    ERROR   → must fix before loading
    WARNING → note it but continue
    INFO    → just logging
    """
    ERROR = "ERROR"
    WARNING = "WARNING"
    INFO = "INFO"


# ── VALIDATION RULE ───────────────────────────────────────
@dataclass
class ValidationRule:
    """
    A single quality check.

    name      : friendly label, e.g. "age_positive"
    check     : a function that takes a DataFrame and returns True (pass) / False (fail)
    severity  : how serious if it fails?
    message   : human-readable explanation of what went wrong
    """
    name: str
    check: Callable[[pd.DataFrame], bool]
    severity: Severity = Severity.ERROR
    message: str = ""


# ── VALIDATION RESULT ────────────────────────────────────
@dataclass
class ValidationResult:
    """
    The outcome of running all rules.
    Like a full report card rather than just one mark.
    """
    passed: bool = True
    errors: List[dict] = field(default_factory=list)
    warnings: List[dict] = field(default_factory=list)
    stats: dict = field(default_factory=dict)

    def add_issue(self, rule: ValidationRule, detail: str = ""):
        entry = {"rule": rule.name, "message": rule.message, "detail": detail}
        if rule.severity == Severity.ERROR:
            self.errors.append(entry)
            self.passed = False
        else:
            self.warnings.append(entry)

    def summary(self) -> str:
        status = "✅ PASSED" if self.passed else "❌ FAILED"
        return (
            f"{status} | "
            f"{len(self.errors)} errors, {len(self.warnings)} warnings"
        )


# ── MAIN VALIDATOR CLASS ──────────────────────────────────
class DataValidator:
    """
    THE QUALITY INSPECTOR.

    Build a set of rules, then run them against any DataFrame.
    """

    def __init__(self, strict_mode: bool = True):
        """
        strict_mode=True  → raise an exception if any ERROR rule fails
        strict_mode=False → log errors but don't stop the pipeline
        """
        self.strict_mode = strict_mode
        self.rules: List[ValidationRule] = []

    # ── ADD RULES ─────────────────────────────────────────
    def require_columns(self, columns: List[str]) -> "DataValidator":
        """RULE: These columns MUST exist."""
        self.rules.append(ValidationRule(
            name="required_columns",
            check=lambda df: all(c in df.columns for c in columns),
            severity=Severity.ERROR,
            message=f"Missing required columns: {columns}",
        ))
        return self

    def no_nulls_in(self, columns: List[str]) -> "DataValidator":
        """RULE: These columns must have zero empty values."""
        for col in columns:
            self.rules.append(ValidationRule(
                name=f"no_nulls:{col}",
                check=lambda df, c=col: df[c].notnull().all() if c in df.columns else True,
                severity=Severity.ERROR,
                message=f"Column '{col}' contains null values",
            ))
        return self

    def max_null_percent(self, column: str, max_pct: float = 0.1) -> "DataValidator":
        """
        RULE: A column can have at most max_pct% empty values.
        e.g., max_null_percent("email", 0.05) → at most 5% can be empty
        """
        self.rules.append(ValidationRule(
            name=f"max_null_pct:{column}",
            check=lambda df, c=column, m=max_pct: (
                df[c].isnull().mean() <= m if c in df.columns else True
            ),
            severity=Severity.WARNING,
            message=f"Column '{column}' has >{max_pct*100:.0f}% null values",
        ))
        return self

    def numeric_range(
        self, column: str, min_val: Optional[float] = None, max_val: Optional[float] = None
    ) -> "DataValidator":
        """
        RULE: All values in a numeric column must be within [min_val, max_val].

        Example:
            .numeric_range("age", 0, 120)
            .numeric_range("temperature", -50, 60)
        """
        def check_range(df: pd.DataFrame) -> bool:
            if column not in df.columns:
                return True
            series = df[column].dropna()
            if min_val is not None and (series < min_val).any():
                return False
            if max_val is not None and (series > max_val).any():
                return False
            return True

        self.rules.append(ValidationRule(
            name=f"numeric_range:{column}",
            check=check_range,
            severity=Severity.ERROR,
            message=f"Column '{column}' has values outside [{min_val}, {max_val}]",
        ))
        return self

    def unique_values(self, column: str) -> "DataValidator":
        """RULE: All values in this column must be unique (no duplicates)."""
        self.rules.append(ValidationRule(
            name=f"unique:{column}",
            check=lambda df, c=column: df[c].nunique() == len(df[c].dropna()) if c in df.columns else True,
            severity=Severity.ERROR,
            message=f"Column '{column}' contains duplicate values",
        ))
        return self

    def valid_email(self, column: str) -> "DataValidator":
        """RULE: Values in this column must look like valid email addresses."""
        pattern = r"^[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}$"

        def check_emails(df: pd.DataFrame) -> bool:
            if column not in df.columns:
                return True
            non_null = df[column].dropna().astype(str)
            return non_null.str.match(pattern).all()

        self.rules.append(ValidationRule(
            name=f"valid_email:{column}",
            check=check_emails,
            severity=Severity.WARNING,
            message=f"Column '{column}' contains invalid email addresses",
        ))
        return self

    def min_row_count(self, minimum: int) -> "DataValidator":
        """RULE: The dataset must have at least `minimum` rows."""
        self.rules.append(ValidationRule(
            name="min_row_count",
            check=lambda df: len(df) >= minimum,
            severity=Severity.ERROR,
            message=f"Dataset has fewer than {minimum} rows",
        ))
        return self

    def custom_rule(self, name: str, check: Callable, message: str = "", severity: Severity = Severity.WARNING) -> "DataValidator":
        """Add any custom rule you can dream up."""
        self.rules.append(ValidationRule(name=name, check=check, severity=severity, message=message))
        return self

    # ── RUN ALL RULES ────────────────────────────────────
    def validate(self, df: pd.DataFrame) -> ValidationResult:
        """
        Run every rule against the DataFrame.
        Returns a ValidationResult with the full report.
        """
        result = ValidationResult()

        # Basic stats — always useful to know
        result.stats = {
            "total_rows": len(df),
            "total_columns": len(df.columns),
            "null_cells": int(df.isnull().sum().sum()),
            "duplicate_rows": int(df.duplicated().sum()),
            "columns": list(df.columns),
        }

        logger.info(f"Running {len(self.rules)} validation rules on {len(df)} rows...")

        for rule in self.rules:
            try:
                passed = rule.check(df)
                if not passed:
                    result.add_issue(rule)
                    logger.log(
                        "ERROR" if rule.severity == Severity.ERROR else "WARNING",
                        f"Validation FAILED [{rule.name}]: {rule.message}"
                    )
                else:
                    logger.debug(f"Validation PASSED [{rule.name}]")
            except Exception as e:
                logger.error(f"Rule '{rule.name}' crashed: {e}")
                result.add_issue(rule, detail=str(e))

        logger.info(f"Validation complete: {result.summary()}")

        if self.strict_mode and not result.passed:
            raise ValueError(f"Data validation failed:\n" + "\n".join(
                f"  ❌ {e['rule']}: {e['message']}" for e in result.errors
            ))

        return result

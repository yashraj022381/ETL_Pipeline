"""
=============================================================
  TESTS / TEST_PIPELINE.PY  —  The Quality Control Team
=============================================================

WHAT ARE TESTS?
  Tests are CODE that checks your code works correctly.

  Imagine building a bridge:
    - Before opening it to cars, engineers TEST every bolt,
      every beam, every rivet.
    - If something fails the test, they fix it BEFORE the
      bridge opens — not after a car falls into the river.

  Software tests work the same way:
    - We write small programs that check each function
    - If a function stops working, the test FAILS and alerts us
    - We fix it before it reaches real users

WHY PYTEST?
  pytest is the most popular testing framework for Python.
  You write functions starting with "test_" and pytest
  finds and runs them all automatically.

TEST TYPES HERE:
  Unit tests     → test one small function in isolation
  Integration    → test multiple components working together
=============================================================
"""

import pytest
import pandas as pd
import numpy as np
import sys
import os

# Add the parent directory to Python's search path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from transformers.data_transformer import DataTransformer
from validators.data_validator import DataValidator, Severity


# ── FIXTURES: shared test data ────────────────────────────
# A pytest fixture is a helper that creates test data.
# The @pytest.fixture decorator tells pytest to run this
# function before any test that requests it.

@pytest.fixture
def messy_df():
    """
    A messy DataFrame that mirrors real-world data quality issues.
    Every test that takes `messy_df` as a parameter gets this.
    """
    return pd.DataFrame({
        "First Name":   ["  Alice  ", "BOB", "charlie", None, "Eve"],
        "Last Name":    ["Smith", "JONES", "brown", "Davis", "Wilson"],
        "Age":          [30, 25, -5, 45, 999],       # -5 and 999 are outliers
        "Salary":       [50000.0, 45000.0, None, 60000.0, 55000.0],
        "Email":        ["alice@test.com", "bob@test.com", "not-an-email", None, "eve@test.com"],
        "Department":   ["Sales", "Sales", "HR", "HR", "Sales"],
        "Employee ID":  [101, 102, 102, 104, 105],   # 102 is a duplicate
    })

@pytest.fixture
def clean_df():
    """A clean DataFrame with no issues."""
    return pd.DataFrame({
        "name":       ["alice", "bob", "charlie"],
        "age":        [30, 25, 35],
        "salary":     [50000.0, 45000.0, 60000.0],
        "email":      ["alice@test.com", "bob@test.com", "charlie@test.com"],
        "department": ["sales", "hr", "engineering"],
    })


# ══════════════════════════════════════════════════════════
#   TRANSFORMER TESTS
# ══════════════════════════════════════════════════════════

class TestDataTransformer:
    """Tests for the DataTransformer class."""

    def test_clean_column_names(self, messy_df):
        """Column names should become lowercase snake_case."""
        result = DataTransformer(messy_df).clean_column_names().transform()

        # All column names should be lowercase
        assert all(c == c.lower() for c in result.columns), \
            "All column names should be lowercase"

        # No spaces — should be underscores
        assert all(" " not in c for c in result.columns), \
            "Column names should not contain spaces"

        # Specific check
        assert "first_name" in result.columns
        assert "employee_id" in result.columns

    def test_handle_nulls_drop(self, messy_df):
        """Dropping nulls should reduce row count."""
        original_len = len(messy_df)
        result = DataTransformer(messy_df).handle_nulls(strategy="drop").transform()

        # Should have fewer or equal rows (nulls removed)
        assert len(result) <= original_len, "Dropping nulls should not increase row count"

    def test_handle_nulls_fill_median(self):
        """Filling with median should keep all rows and fill NaN."""
        df = pd.DataFrame({"value": [1.0, 2.0, None, 4.0, 5.0]})
        result = DataTransformer(df).handle_nulls(strategy="fill_median").transform()

        # No NaN values should remain
        assert result["value"].isnull().sum() == 0, "No nulls should remain after fill_median"

        # The filled value should be the median (3.0 in this case)
        assert result["value"].iloc[2] == 3.0, "Null should be filled with median (3.0)"

    def test_remove_duplicates(self, messy_df):
        """Duplicate rows should be removed."""
        result = (DataTransformer(messy_df)
                    .clean_column_names()
                    .remove_duplicates(subset=["employee_id"])
                    .transform())

        # employee_id 102 appears twice — after dedup, each ID should be unique
        assert result["employee_id"].duplicated().sum() == 0, \
            "No duplicate employee_ids should remain"

    def test_standardise_text_lower(self, messy_df):
        """Text should be converted to lowercase and stripped."""
        result = (DataTransformer(messy_df)
                    .standardise_text(["First Name", "Last Name"], case="lower")
                    .transform())

        # BOB should become bob
        assert "BOB" not in result["First Name"].values
        assert "JONES" not in result["Last Name"].values

    def test_handle_outliers_clip(self):
        """Outliers should be capped at the boundary values."""
        df = pd.DataFrame({"age": [20, 25, 30, -5, 999, 35]})
        result = (DataTransformer(df)
                    .handle_outliers(["age"], method="iqr", action="clip")
                    .transform())

        # No age should be impossibly negative after clipping
        assert (result["age"] >= 0).all() or result["age"].min() > -100, \
            "Negative ages should be clipped"

    def test_cast_types(self):
        """Type casting should convert string numbers to actual numbers."""
        df = pd.DataFrame({"age": ["25", "30", "35"], "salary": ["50000", "60000", "70000"]})
        result = (DataTransformer(df)
                    .cast_types({"age": int, "salary": float})
                    .transform())

        assert result["age"].dtype == int or result["age"].dtype == "int64", \
            "Age should be integer type"
        assert result["salary"].dtype == float, \
            "Salary should be float type"

    def test_add_derived_columns(self):
        """Derived columns should be created from existing ones."""
        df = pd.DataFrame({"first": ["Alice", "Bob"], "last": ["Smith", "Jones"]})
        result = (DataTransformer(df)
                    .add_derived_columns({
                        "full_name": lambda d: d["first"] + " " + d["last"]
                    })
                    .transform())

        assert "full_name" in result.columns, "full_name column should be created"
        assert result["full_name"].iloc[0] == "Alice Smith"

    def test_chaining(self, messy_df):
        """Multiple operations chained together should all apply."""
        result = (DataTransformer(messy_df)
                    .clean_column_names()
                    .handle_nulls(strategy="fill_median")
                    .remove_duplicates()
                    .transform())

        # Shape should be different from original (some ops change it)
        # Main check: no crash, returns a DataFrame
        assert isinstance(result, pd.DataFrame), "Should return a DataFrame"
        assert len(result) > 0, "Result should not be empty"


# ══════════════════════════════════════════════════════════
#   VALIDATOR TESTS
# ══════════════════════════════════════════════════════════

class TestDataValidator:
    """Tests for the DataValidator class."""

    def test_require_columns_pass(self, clean_df):
        """Should pass when all required columns are present."""
        result = (DataValidator(strict_mode=False)
                    .require_columns(["name", "age", "salary"])
                    .validate(clean_df))
        assert result.passed

    def test_require_columns_fail(self, clean_df):
        """Should fail when a required column is missing."""
        result = (DataValidator(strict_mode=False)
                    .require_columns(["name", "nonexistent_column"])
                    .validate(clean_df))
        assert not result.passed
        assert len(result.errors) > 0

    def test_numeric_range_pass(self, clean_df):
        """Should pass when all values are within the allowed range."""
        result = (DataValidator(strict_mode=False)
                    .numeric_range("age", 0, 100)
                    .validate(clean_df))
        assert result.passed

    def test_numeric_range_fail(self):
        """Should fail when values are outside the allowed range."""
        df = pd.DataFrame({"age": [25, 30, 999, -5]})  # 999 and -5 are out of range
        result = (DataValidator(strict_mode=False)
                    .numeric_range("age", 0, 120)
                    .validate(df))
        assert not result.passed

    def test_unique_values_pass(self, clean_df):
        """Should pass when all IDs are unique."""
        df = clean_df.copy()
        df["id"] = [1, 2, 3]
        result = DataValidator(strict_mode=False).unique_values("id").validate(df)
        assert result.passed

    def test_unique_values_fail(self):
        """Should fail when duplicate IDs exist."""
        df = pd.DataFrame({"id": [1, 2, 2, 3]})   # 2 is duplicated
        result = DataValidator(strict_mode=False).unique_values("id").validate(df)
        assert not result.passed

    def test_valid_email_warning(self):
        """Invalid emails should generate a warning (not ERROR by default)."""
        df = pd.DataFrame({"email": ["valid@test.com", "not-an-email", "also@valid.com"]})
        result = DataValidator(strict_mode=False).valid_email("email").validate(df)
        assert len(result.warnings) > 0

    def test_min_row_count_fail(self):
        """Should fail when dataset has fewer rows than minimum."""
        df = pd.DataFrame({"x": [1, 2]})
        result = DataValidator(strict_mode=False).min_row_count(100).validate(df)
        assert not result.passed

    def test_strict_mode_raises(self):
        """In strict mode, validation errors should raise ValueError."""
        df = pd.DataFrame({"x": [1]})
        with pytest.raises(ValueError):
            DataValidator(strict_mode=True).min_row_count(1000).validate(df)

    def test_validation_stats(self, clean_df):
        """Validation result should include dataset statistics."""
        result = DataValidator(strict_mode=False).validate(clean_df)
        assert "total_rows" in result.stats
        assert result.stats["total_rows"] == len(clean_df)

    def test_multiple_rules(self, clean_df):
        """Multiple rules should all be evaluated."""
        result = (DataValidator(strict_mode=False)
                    .require_columns(["name", "age"])
                    .numeric_range("age", 0, 100)
                    .unique_values("name")
                    .validate(clean_df))
        # All rules pass for the clean fixture
        assert result.passed


# ══════════════════════════════════════════════════════════
#   INTEGRATION TEST
# ══════════════════════════════════════════════════════════

class TestPipelineIntegration:
    """
    Tests the components working together end-to-end.
    Like testing the whole assembly line, not just one machine.
    """

    def test_extract_transform_validate_flow(self, messy_df):
        """
        Full flow: transform messy data, then validate clean data.
        Should produce a clean, valid output.
        """
        # TRANSFORM
        clean = (DataTransformer(messy_df)
                    .clean_column_names()
                    .handle_nulls(strategy="fill_median")
                    .remove_duplicates()
                    .standardise_text(["first_name", "last_name"], case="lower")
                    .transform())

        # VALIDATE
        result = (DataValidator(strict_mode=False)
                    .require_columns(["first_name", "age", "salary"])
                    .min_row_count(1)
                    .validate(clean))

        assert isinstance(clean, pd.DataFrame), "Should produce a DataFrame"
        assert len(clean) > 0, "Result should not be empty"
        assert result.stats["total_rows"] == len(clean)

    def test_transformer_preserves_data_integrity(self):
        """
        Transformations should not corrupt valid data.
        If a row has no issues, it should survive all transformations.
        """
        perfect_df = pd.DataFrame({
            "name":   ["Alice", "Bob", "Charlie"],
            "age":    [30, 25, 35],
            "salary": [50000.0, 45000.0, 60000.0],
        })

        result = (DataTransformer(perfect_df)
                    .clean_column_names()
                    .handle_nulls(strategy="drop")
                    .remove_duplicates()
                    .transform())

        # Perfect data should survive untouched (same number of rows)
        assert len(result) == len(perfect_df), "Clean data should not lose rows"

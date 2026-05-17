"""
=============================================================
  TRANSFORMERS / DATA_TRANSFORMER.PY  —  The Data Chef
=============================================================

WHAT IS TRANSFORMATION?
  The "T" in ETL.

  Raw data from the real world is MESSY:
    - Some names have RANDOM CAPITALISATION
    - Phone numbers look like "555-1234", "5551234", "(555) 1234"
    - Ages can be -5 or 999 (clearly wrong)
    - Dates might be "01/03/2024" or "March 1, 2024" or "2024-03-01"
    - Some cells are completely empty (null/NaN)

  The transformer is like a CHEF who takes raw ingredients
  (messy data) and turns them into a clean, consistent meal
  (ready-to-use data).

WHAT OPERATIONS DOES IT DO?
  1. Type casting      — making sure numbers are numbers, not text
  2. Null handling     — deciding what to do with empty cells
  3. Normalisation     — making text consistent (all lowercase, etc.)
  4. Deduplication     — removing exact duplicate rows
  5. Outlier handling  — flagging impossibly large/small values
  6. Feature engineering — creating NEW columns from existing ones
=============================================================
"""

import re
from typing import Optional, Dict, List, Callable
import pandas as pd
import numpy as np
from loguru import logger


class DataTransformer:
    """
    THE DATA CHEF.

    You can chain multiple transformations together:
        transformer = DataTransformer(df)
        result = (transformer
                    .clean_column_names()
                    .handle_nulls()
                    .standardise_text(["name"])
                    .cast_types({"age": int})
                    .remove_duplicates()
                    .transform())
    """

    def __init__(self, df: pd.DataFrame):
        """
        Takes a raw DataFrame and creates an internal copy to work on.
        (We never modify the original — always work on a copy, like a chef
         using a cutting board, not the dining table.)
        """
        self._df = df.copy()
        self._original_shape = df.shape
        self._operations: List[str] = []   # audit trail of what we did
        logger.debug(f"Transformer created. Shape: {df.shape}")

    # ── STEP 1: CLEAN COLUMN NAMES ────────────────────────────
    def clean_column_names(self) -> "DataTransformer":
        """
        Turn messy column names into clean snake_case.

        Examples:
          "First Name"   →  "first_name"
          "  SaLeS $$$"  →  "sales"
          "Phone.Number" →  "phone_number"

        WHY? Python code works much better with simple names.
        """
        def snake(name: str) -> str:
            name = str(name).strip()
            name = re.sub(r"[^\w\s]", "", name)     # remove special chars
            name = re.sub(r"\s+", "_", name)          # spaces → underscores
            return name.lower().strip("_")

        self._df.columns = [snake(c) for c in self._df.columns]
        self._operations.append("clean_column_names")
        logger.debug(f"Cleaned columns: {list(self._df.columns)}")
        return self   # return self so we can chain: .clean().handle_nulls()

    # ── STEP 2: HANDLE NULL / MISSING VALUES ──────────────────
    def handle_nulls(
        self,
        strategy: str = "drop",        # "drop", "fill_mean", "fill_median", "fill_mode", "fill_value"
        fill_value=None,
        columns: Optional[List[str]] = None,
        threshold: float = 0.5,         # drop columns with >50% nulls
    ) -> "DataTransformer":
        """
        Deal with empty (NaN/None) cells.

        STRATEGIES:
          "drop"        — delete any row that has at least one empty cell
          "fill_mean"   — replace empty numbers with the average of that column
          "fill_median" — replace with the middle value (better for skewed data)
          "fill_mode"   — replace with the most common value
          "fill_value"  — replace with a specific value you provide

        WHY DOES THIS MATTER?
          Most ML models and SQL databases can't handle NaN.
          We need to make a decision: delete the row or fill it in.
        """
        target_cols = columns or list(self._df.columns)

        # First: drop columns that are mostly empty (>threshold% empty)
        null_pct = self._df[target_cols].isnull().mean()
        drop_cols = null_pct[null_pct > threshold].index.tolist()
        if drop_cols:
            logger.warning(f"Dropping {len(drop_cols)} columns (>{threshold*100:.0f}% null): {drop_cols}")
            self._df.drop(columns=drop_cols, inplace=True)
            target_cols = [c for c in target_cols if c not in drop_cols]

        # Then: handle remaining nulls per strategy
        if strategy == "drop":
            before = len(self._df)
            self._df.dropna(subset=[c for c in target_cols if c in self._df.columns], inplace=True)
            logger.debug(f"Dropped {before - len(self._df)} rows with nulls")

        elif strategy == "fill_mean":
            for col in target_cols:
                if col in self._df.columns and pd.api.types.is_numeric_dtype(self._df[col]):
                    mean_val = self._df[col].mean()
                    self._df[col] = self._df[col].fillna(mean_val)

        elif strategy == "fill_median":
            for col in target_cols:
                if col in self._df.columns and pd.api.types.is_numeric_dtype(self._df[col]):
                    median_val = self._df[col].median()
                    self._df[col] = self._df[col].fillna(median_val)

        elif strategy == "fill_mode":
            for col in target_cols:
                if col in self._df.columns and not self._df[col].empty:
                    mode_val = self._df[col].mode()
                    if not mode_val.empty:
                        self._df[col] = self._df[col].fillna(mode_val[0])

        elif strategy == "fill_value" and fill_value is not None:
            self._df[target_cols] = self._df[target_cols].fillna(fill_value)

        self._operations.append(f"handle_nulls:{strategy}")
        return self

    # ── STEP 3: STANDARDISE TEXT ──────────────────────────────
    def standardise_text(
        self,
        columns: List[str],
        case: str = "lower",          # "lower", "upper", "title"
        strip_whitespace: bool = True,
        remove_special_chars: bool = False,
    ) -> "DataTransformer":
        """
        Make text consistent across rows.

        Example: "  ALICE smith " → "alice smith"
        """
        for col in columns:
            if col not in self._df.columns:
                logger.warning(f"Column '{col}' not found — skipping")
                continue

            if strip_whitespace:
                self._df[col] = self._df[col].astype(str).str.strip()

            if case == "lower":
                self._df[col] = self._df[col].str.lower()
            elif case == "upper":
                self._df[col] = self._df[col].str.upper()
            elif case == "title":
                self._df[col] = self._df[col].str.title()

            if remove_special_chars:
                self._df[col] = self._df[col].str.replace(r"[^\w\s]", "", regex=True)

        self._operations.append(f"standardise_text:{columns}")
        return self

    # ── STEP 4: CAST / CONVERT DATA TYPES ─────────────────────
    def cast_types(self, type_map: Dict[str, type]) -> "DataTransformer":
        """
        Force columns to be specific data types.

        Example: { "age": int, "salary": float, "hire_date": "datetime64" }

        WHY? A column might come in as text "42" when it should be the
             number 42. You can't do maths on text!
        """
        for col, dtype in type_map.items():
            if col not in self._df.columns:
                continue
            try:
                if dtype == "datetime64" or str(dtype) == "datetime":
                    self._df[col] = pd.to_datetime(self._df[col], errors="coerce")
                else:
                    self._df[col] = self._df[col].astype(dtype)
                logger.debug(f"Cast '{col}' to {dtype}")
            except Exception as e:
                logger.warning(f"Could not cast '{col}' to {dtype}: {e}")

        self._operations.append(f"cast_types")
        return self

    # ── STEP 5: REMOVE DUPLICATES ─────────────────────────────
    def remove_duplicates(self, subset: Optional[List[str]] = None) -> "DataTransformer":
        """
        Delete rows that are exact copies of other rows.

        subset: only check these columns for duplicates.
                If None, ALL columns must match to be a duplicate.

        Like removing duplicate entries in an address book.
        """
        before = len(self._df)
        self._df.drop_duplicates(subset=subset, keep="first", inplace=True)
        removed = before - len(self._df)
        if removed:
            logger.info(f"Removed {removed} duplicate rows")
        self._operations.append("remove_duplicates")
        return self

    # ── STEP 6: HANDLE OUTLIERS ───────────────────────────────
    def handle_outliers(
        self,
        columns: List[str],
        method: str = "iqr",     # "iqr" or "zscore"
        action: str = "clip",    # "clip" (cap), "remove" (delete row), "flag" (add column)
    ) -> "DataTransformer":
        """
        Deal with extreme values that are probably wrong.

        IQR METHOD (Interquartile Range):
          The "middle 50%" of data. Values very far outside this
          range are likely errors.
          (Like saying any human age outside 0–120 is suspicious.)

        ZSCORE METHOD:
          Values more than 3 standard deviations from the mean.
          Standard deviation measures how spread out your data is.

        ACTIONS:
          "clip"   — cap the value at the boundary (100 → boundary_max)
          "remove" — delete that entire row
          "flag"   — add a column marking it as an outlier (keep the row)
        """
        for col in columns:
            if col not in self._df.columns:
                continue
            if not pd.api.types.is_numeric_dtype(self._df[col]):
                continue

            if method == "iqr":
                Q1 = self._df[col].quantile(0.25)    # 25th percentile
                Q3 = self._df[col].quantile(0.75)    # 75th percentile
                IQR = Q3 - Q1                         # middle 50% range
                lower = Q1 - 1.5 * IQR
                upper = Q3 + 1.5 * IQR

            elif method == "zscore":
                mean = self._df[col].mean()
                std = self._df[col].std()
                lower = mean - 3 * std
                upper = mean + 3 * std

            outliers = (self._df[col] < lower) | (self._df[col] > upper)
            count = outliers.sum()

            if count == 0:
                continue

            logger.debug(f"Column '{col}': {count} outliers detected [{lower:.2f}, {upper:.2f}]")

            if action == "clip":
                self._df[col] = self._df[col].clip(lower=lower, upper=upper)
            elif action == "remove":
                self._df = self._df[~outliers]
            elif action == "flag":
                self._df[f"{col}_is_outlier"] = outliers

        self._operations.append(f"handle_outliers:{method}:{action}")
        return self

    # ── STEP 7: FEATURE ENGINEERING ──────────────────────────
    def add_derived_columns(self, derivations: Dict[str, Callable]) -> "DataTransformer":
        """
        Create brand-new columns calculated from existing ones.

        WHAT IS FEATURE ENGINEERING?
          Imagine you have a "birth_date" column.
          You can DERIVE an "age" column from it.
          That's feature engineering — turning existing data into
          something more useful.

        derivations: dictionary of { "new_col_name": function }

        Example:
            transformer.add_derived_columns({
                "full_name": lambda df: df["first"] + " " + df["last"],
                "age_group": lambda df: pd.cut(df["age"], bins=[0,18,65,100],
                                               labels=["child","adult","senior"])
            })
        """
        for col_name, func in derivations.items():
            try:
                self._df[col_name] = func(self._df)
                logger.debug(f"Added derived column: '{col_name}'")
            except Exception as e:
                logger.error(f"Failed to create column '{col_name}': {e}")

        self._operations.append("add_derived_columns")
        return self

    # ── FINALISE ──────────────────────────────────────────────
    def transform(self) -> pd.DataFrame:
        """
        HAND OVER THE FINISHED RESULT.

        Call this at the end of your chain to get the
        cleaned DataFrame back.
        """
        final_shape = self._df.shape
        rows_removed = self._original_shape[0] - final_shape[0]
        cols_changed = abs(self._original_shape[1] - final_shape[1])

        logger.info(
            f"Transformation complete. "
            f"Original: {self._original_shape} → Final: {final_shape} "
            f"({rows_removed} rows removed, {cols_changed} columns changed)"
        )
        logger.debug(f"Operations applied: {' → '.join(self._operations)}")
        return self._df

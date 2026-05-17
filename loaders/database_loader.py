"""
=============================================================
  LOADERS / DATABASE_LOADER.PY  —  The Data Warehouse Clerk
=============================================================

WHAT IS LOADING?
  The "L" in ETL.

  After extracting raw data and transforming it to be clean
  and consistent, we LOAD it into its permanent home: a
  database or file.

  Like after organising all your files, putting them into the
  right filing cabinets.

WHAT IS SQLALCHEMY?
  A Python library that lets us talk to many different databases
  (PostgreSQL, MySQL, SQLite) using the SAME Python code.
  We don't need to learn each database's specific dialect.

LOAD STRATEGIES:
  "append"   → add new rows to existing data (like adding pages to a book)
  "replace"  → delete everything and start fresh (like rewriting the book)
  "upsert"   → update rows that exist, insert rows that don't
               (like a smart merge — the most advanced option)
=============================================================
"""

from typing import Optional, Literal
import pandas as pd
from sqlalchemy import create_engine, text, inspect
from sqlalchemy.exc import SQLAlchemyError
from loguru import logger
from config.settings import Config


class DatabaseLoader:
    """
    THE DATABASE WRITER.

    Takes clean DataFrames and saves them to a database.
    Supports append, replace, and upsert strategies.
    """

    def __init__(
        self,
        connection_string: Optional[str] = None,
        schema: Optional[str] = None,
    ):
        """
        connection_string: full DB URL. If None, uses Config default.
        schema: database schema (like a folder inside the database)
        """
        # Try PostgreSQL first; fall back to SQLite for demo/testing
        try:
            conn_str = connection_string or Config.database.url
            self.engine = create_engine(conn_str, pool_pre_ping=True, echo=False)
            # Test the connection
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            self.db_type = "postgresql"
        except Exception:
            logger.warning("PostgreSQL unavailable — using SQLite fallback")
            self.engine = create_engine(Config.database.url_sqlite)
            self.db_type = "sqlite"

        self.schema = schema
        logger.info(f"DatabaseLoader ready ({self.db_type})")

    # ── LOAD: APPEND / REPLACE ────────────────────────────────
    def load(
        self,
        df: pd.DataFrame,
        table_name: str,
        if_exists: Literal["append", "replace", "fail"] = "append",
        chunk_size: int = 500,
        add_timestamps: bool = True,
    ) -> int:
        """
        Write a DataFrame to a database table.

        df          : the cleaned data to save
        table_name  : which table to write to
        if_exists   : what to do if the table already exists
        chunk_size  : write N rows at a time (avoids memory spikes)
        add_timestamps : add "loaded_at" column automatically

        Returns the number of rows written.
        """
        if df.empty:
            logger.warning(f"Empty DataFrame — nothing to load into '{table_name}'")
            return 0

        # Optionally stamp each row with when it was loaded
        if add_timestamps:
            df = df.copy()
            df["_loaded_at"] = pd.Timestamp.now()

        # Remove metadata columns we added during extraction
        meta_cols = [c for c in df.columns if c.startswith("_source") or c.startswith("_batch") or c.startswith("_extracted")]
        if meta_cols:
            df = df.drop(columns=meta_cols, errors="ignore")

        try:
            import sqlite3

            raw_conn = sqlite3.connect("etl_pipeline.db")

            df.to_sql(
                name=table_name,
                con=raw_conn,
                if_exists=if_exists,
                index=False,
                chunksize=chunk_size,
            )

            raw_conn.commit()
            raw_conn.close()
            
            logger.info(f"Loaded {len(df):,} rows → table '{table_name}'") # (strategy: {if_exists})
            return len(df)

        except SQLAlchemyError as e:
            logger.error(f"Database load failed for table '{table_name}': {e}")
            raise

    # ── LOAD: UPSERT (advanced) ────────────────────────────────
    def upsert(
        self,
        df: pd.DataFrame,
        table_name: str,
        unique_columns: list,
        chunk_size: int = 500,
    ) -> dict:
        """
        UPSERT = UPDATE + INSERT combined.

        For each incoming row:
          - If a row with the same unique_columns values EXISTS → UPDATE it
          - If it does NOT exist → INSERT it

        This is the most advanced load strategy and prevents duplicates
        even when the pipeline runs multiple times.

        EXAMPLE:
          unique_columns = ["employee_id"]
          If employee_id=42 already exists → update their salary
          If employee_id=99 doesn't exist  → insert new row

        NOTE: This uses a "staging table" pattern:
          1. Load new data into a temp table
          2. Run a SQL MERGE/INSERT…ON CONFLICT from temp → final table
          3. Drop the temp table
        """
        if df.empty:
            return {"inserted": 0, "updated": 0}

        if self.db_type == "sqlite":
            # SQLite doesn't support PostgreSQL ON CONFLICT syntax
            # Fall back to simple append for demo purposes
            logger.warning("Upsert not fully supported on SQLite — using append")
            inserted = self.load(df, table_name, if_exists="append", chunk_size=chunk_size)
            return {"inserted": inserted, "updated": 0}

        temp_table = f"_staging_{table_name}"

        try:
            # Step 1: Load into staging table
            df.to_sql(temp_table, self.engine, if_exists="replace", index=False, chunksize=chunk_size)

            with self.engine.begin() as conn:  # begin() auto-commits on success
                # Step 2: Ensure the main table exists (create from staging if not)
                inspector = inspect(self.engine)
                if not inspector.has_table(table_name):
                    conn.execute(text(f"CREATE TABLE {table_name} AS SELECT * FROM {temp_table} WHERE 1=0"))

                # Step 3: Build PostgreSQL ON CONFLICT upsert
                columns = [c for c in df.columns if not c.startswith("_")]
                non_key_cols = [c for c in columns if c not in unique_columns]
                conflict_cols = ", ".join(unique_columns)

                update_clause = ", ".join(f"{c} = EXCLUDED.{c}" for c in non_key_cols)
                insert_cols = ", ".join(columns)
                select_cols = ", ".join(columns)

                sql = f"""
                    INSERT INTO {table_name} ({insert_cols})
                    SELECT {select_cols} FROM {temp_table}
                    ON CONFLICT ({conflict_cols})
                    DO UPDATE SET {update_clause}
                """
                result = conn.execute(text(sql))
                rows_affected = result.rowcount

                # Step 4: Drop the temporary staging table
                conn.execute(text(f"DROP TABLE IF EXISTS {temp_table}"))

            logger.info(f"Upsert complete: {rows_affected} rows affected in '{table_name}'")
            return {"inserted": rows_affected, "updated": 0}  # PG doesn't easily split these

        except SQLAlchemyError as e:
            logger.error(f"Upsert failed: {e}")
            raise

    # ── SAVE TO FILE (alternative to database) ────────────────
    def save_to_parquet(self, df: pd.DataFrame, path: str):
        """
        Save as a Parquet file — a compressed, fast binary format.

        WHY PARQUET?
          - 5-10x smaller than CSV
          - Stores data types (so you don't lose them)
          - Very fast to read back into pandas
          - Industry standard for data engineering
        """
        from pathlib import Path
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        df.to_parquet(path, index=False, compression="snappy")
        size_mb = Path(path).stat().st_size / 1_000_000
        logger.info(f"Saved {len(df):,} rows to Parquet: {path} ({size_mb:.2f} MB)")

    def save_to_csv(self, df: pd.DataFrame, path: str):
        """Save as CSV (readable by humans and Excel)."""
        from pathlib import Path
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(path, index=False)
        logger.info(f"Saved {len(df):,} rows to CSV: {path}")

    # ── QUERY HELPER ─────────────────────────────────────────
    def query(self, sql: str) -> pd.DataFrame:
        """
        Run a SELECT query and get the results as a DataFrame.
        Useful for spot-checking: "did the data load correctly?"
        """
        with self.engine.connect() as conn:
            return pd.read_sql(text(sql), conn)

    def table_exists(self, table_name: str) -> bool:
        inspector = inspect(self.engine)
        return inspector.has_table(table_name)

    def row_count(self, table_name: str) -> int:
        if not self.table_exists(table_name):
            return 0
        with self.engine.connect() as conn:
            result = conn.execute(text(f"SELECT COUNT(*) FROM {table_name}"))
            return result.scalar()

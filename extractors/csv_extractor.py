"""
=============================================================
  EXTRACTORS / CSV_EXTRACTOR.PY  —  The CSV File Reader
=============================================================

WHAT IS A CSV FILE?
  CSV = Comma-Separated Values.
  It's the simplest possible spreadsheet — just text with
  commas between values:

    name,age,salary
    Alice,30,50000
    Bob,25,45000

  You can open one in Excel or Notepad.

WHAT DOES THIS FILE DO?
  It reads CSV files in CHUNKS (batches) so we never run out
  of memory, even if the file has 10 million rows.

  Like reading a huge book 50 pages at a time instead of
  trying to memorise the whole thing at once.
=============================================================
"""

from pathlib import Path
from typing import Iterator
import pandas as pd
from loguru import logger
from .base_extractor import BaseExtractor


class CSVExtractor(BaseExtractor):
    """
    Reads CSV files in memory-safe batches.

    Inherits from BaseExtractor — it has all the same
    abilities plus CSV-specific ones.
    """

    def __init__(
        self,
        file_path: str,
        batch_size: int = 1000,
        encoding: str = "utf-8",
        delimiter: str = ",",
        skip_rows: int = 0,
    ):
        """
        SETUP:
        file_path  : where the CSV lives on disk
        batch_size : rows per chunk
        encoding   : character set (utf-8 handles most languages)
        delimiter  : what separates columns (usually comma, sometimes semicolon)
        skip_rows  : skip the first N rows (useful for files with a header block)
        """
        super().__init__(source_name=f"CSV:{file_path}", batch_size=batch_size)
        self.file_path = Path(file_path)
        self.encoding = encoding
        self.delimiter = delimiter
        self.skip_rows = skip_rows
        self._total_rows = 0

    # ── CONNECT ───────────────────────────────────────────────
    def connect(self) -> bool:
        """
        Check the file actually exists before we try to read it.
        Like checking the fridge is there before opening it.
        """
        if not self.file_path.exists():
            logger.error(f"CSV file not found: {self.file_path}")
            return False

        # Count total rows so we can show a progress percentage
        # We do a quick scan with a generator — very memory-efficient
        with open(self.file_path, encoding=self.encoding) as f:
            # subtract 1 for the header row
            self._total_rows = sum(1 for _ in f) - 1 - self.skip_rows

        self._connected = True
        logger.info(f"Connected to CSV: {self.file_path} ({self._total_rows:,} rows)")
        return True

    # ── EXTRACT ───────────────────────────────────────────────
    def extract(self) -> Iterator[pd.DataFrame]:
        """
        Read the CSV file in chunks and yield each chunk.

        pd.read_csv with chunksize returns a TextFileReader —
        basically a conveyor belt that delivers DataFrames one
        batch at a time.
        """
        if not self._connected:
            raise RuntimeError("Call connect() before extract()")

        logger.info(f"Starting extraction from {self.file_path}")
        chunk_number = 0

        try:
            # chunksize tells pandas: "give me 1,000 rows at a time"
            reader = pd.read_csv(
                self.file_path,
                chunksize=self.batch_size,
                encoding=self.encoding,
                delimiter=self.delimiter,
                skiprows=range(1, self.skip_rows + 1) if self.skip_rows else None,
                on_bad_lines="warn",   # don't crash on malformed lines; warn instead
                low_memory=False,      # better type detection for mixed columns
            )

            for chunk in reader:
                chunk_number += 1

                # Add metadata columns so we know where each row came from
                chunk["_source_file"] = self.file_path.name
                chunk["_batch_number"] = chunk_number
                chunk["_extracted_at"] = pd.Timestamp.now()

                logger.debug(f"Extracted batch {chunk_number}: {len(chunk)} rows")
                yield chunk   # ← hand this batch to whoever is calling us

        except Exception as e:
            logger.error(f"Extraction failed at batch {chunk_number}: {e}")
            raise

        logger.info(f"CSV extraction complete. {chunk_number} batches delivered.")

    # ── CLOSE ─────────────────────────────────────────────────
    def close(self):
        """Nothing to close for a file — pandas handles it automatically."""
        self._connected = False
        logger.debug(f"CSV extractor closed: {self.file_path}")

    # ── BONUS: PREVIEW ────────────────────────────────────────
    def preview(self, rows: int = 5) -> pd.DataFrame:
        """
        Peek at the first few rows without reading the whole file.
        Super useful for debugging — like reading the first page of a book.
        """
        return pd.read_csv(
            self.file_path,
            nrows=rows,
            encoding=self.encoding,
            delimiter=self.delimiter,
        )

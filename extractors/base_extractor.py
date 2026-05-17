"""
=============================================================
  EXTRACTORS / BASE_EXTRACTOR.PY  —  The Ingredient Collector
=============================================================

WHAT IS EXTRACTION?
  The "E" in ETL.

  Imagine you're making a smoothie.  The EXTRACTION step is
  grabbing the fruits from the fridge, the freezer, and the
  fruit bowl.  You haven't blended anything yet — you're just
  COLLECTING the raw ingredients.

  In data terms, we're pulling data from:
    - CSV files  (like Excel but simpler)
    - JSON files (text that looks like a Python dictionary)
    - APIs       (websites that give back data, not web pages)
    - Databases  (giant organised tables)

WHAT IS AN ABSTRACT BASE CLASS?
  Think of it as a JOB DESCRIPTION.
  "Anyone who wants to be an Extractor MUST know how to:
    1. connect()  — open the data source
    2. extract()  — grab the data
    3. close()    — tidy up afterwards"

  We never hire the base class itself; we hire specific types
  (CSVExtractor, APIExtractor, etc.) that follow the job spec.
=============================================================
"""

from abc import ABC, abstractmethod
from typing import Iterator
import pandas as pd
from loguru import logger


class BaseExtractor(ABC):
    """
    THE JOB DESCRIPTION FOR ALL EXTRACTORS.

    ABC = Abstract Base Class.
    Think of it as a contract every extractor must sign.
    """

    def __init__(self, source_name: str, batch_size: int = 1000):
        """
        CONSTRUCTOR — runs when we create a new extractor.

        source_name : a friendly label, e.g. "Sales CSV 2024"
        batch_size  : how many rows to read at a time
                      (reading 1 million rows at once would crash your computer;
                       reading 1,000 at a time is much safer)
        """
        self.source_name = source_name
        self.batch_size = batch_size
        self._connected = False   # flag: have we opened the source yet?
        logger.debug(f"Extractor initialised: {source_name}")

    # ── ABSTRACT METHODS (must be implemented by child classes) ──

    @abstractmethod
    def connect(self) -> bool:
        """
        Open the data source.
        Like opening the fridge before you can grab the fruit.
        Returns True if successful, False if something went wrong.
        """
        pass

    @abstractmethod
    def extract(self) -> Iterator[pd.DataFrame]:
        """
        Yield (hand over) data in chunks.

        WHAT IS A GENERATOR / ITERATOR?
          Instead of returning ALL data at once (dangerous for huge files),
          we use 'yield' to hand over one chunk at a time, then pause.
          It's like a conveyor belt: one box at a time, not all at once.

          Example:
            for chunk in extractor.extract():
                process(chunk)   # handle 1,000 rows, then get the next 1,000
        """
        pass

    @abstractmethod
    def close(self):
        """Close the connection — like putting the fridge back."""
        pass

    # ── CONTEXT MANAGER SUPPORT ───────────────────────────────
    # This lets us write:   with CSVExtractor(...) as ext:
    # Python automatically calls connect() and close() for us.

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        # Returning False means "don't hide any errors"
        return False

    # ── SHARED HELPER ─────────────────────────────────────────
    def get_schema(self, df: pd.DataFrame) -> dict:
        """
        Peek at the structure of our data.

        Like reading the table of contents of a book before
        you start reading it.

        Returns a dictionary like:
          { "name": "string", "age": "int", "salary": "float" }
        """
        return {col: str(dtype) for col, dtype in df.dtypes.items()}

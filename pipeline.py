"""
=============================================================
  PIPELINE.PY  —  The Factory Floor Manager
=============================================================

WHAT IS AN ETL PIPELINE?
  ETL = Extract → Transform → Load

  Think of it like a water treatment plant:
    1. EXTRACT  — pump in dirty water from the river      (raw data)
    2. TRANSFORM — filter, clean, and purify it            (clean data)
    3. LOAD     — pump clean water to homes and offices    (save it)

  This file is the MANAGER who coordinates all the workers
  (extractors, transformers, validators, loaders).

  It also handles:
    ✅ Error recovery (what if a step fails?)
    ✅ Progress tracking (how far along are we?)
    ✅ Metrics collection (how many rows? how long?)
    ✅ Multiple data sources in one run
=============================================================
"""

import time
from pathlib import Path
from typing import Optional, List
import pandas as pd
from loguru import logger
from rich.console import Console
from rich.panel import Panel

from config.settings import Config
from extractors.csv_extractor import CSVExtractor
from extractors.api_extractor import APIExtractor
from transformers.data_transformer import DataTransformer
from validators.data_validator import DataValidator, Severity
from loaders.database_loader import DatabaseLoader
from monitoring.logger import MetricsCollector, get_progress_bar

console = Console()


class ETLPipeline:
    """
    THE FULL PIPELINE ORCHESTRATOR.

    One class to rule them all — it connects every component
    and runs the whole E→T→L flow.
    """

    def __init__(self, pipeline_name: str = "default_pipeline"):
        self.name = pipeline_name
        self.metrics = MetricsCollector(pipeline_name)
        self.loader = DatabaseLoader()
        logger.info(f"Pipeline '{pipeline_name}' initialised (v{Config.VERSION})")

    # ══════════════════════════════════════════════════════
    #   FULL PIPELINE RUN
    # ══════════════════════════════════════════════════════
    def run(self, source: str = "demo", target_table: str = "etl_output"):
        """
        Run the complete ETL pipeline end-to-end.

        source: "demo" generates fake data for testing
                "csv"  reads from a CSV file
                "api"  pulls from a REST API
        """
        console.print(Panel.fit(
            f"[bold cyan]🚀 Starting ETL Pipeline: {self.name}[/]",
            border_style="cyan"
        ))

        try:
            # ── STEP 1: EXTRACT ───────────────────────────
            self.metrics.start_stage("extract")
            raw_chunks = self._extract(source)
            self.metrics.end_stage("extract")

            # ── STEP 2: TRANSFORM + VALIDATE + LOAD ───────
            # We process chunk by chunk to stay memory-safe
            total_loaded = 0
            failed_chunks = []

            with get_progress_bar() as progress:
                task = progress.add_task("[cyan]Processing batches...", total=None)

                for i, chunk in enumerate(raw_chunks):
                    chunk_id = f"batch_{i+1}"
                    progress.update(task, description=f"[cyan]Processing {chunk_id} ({len(chunk)} rows)...")

                    try:
                        # TRANSFORM
                        self.metrics.start_stage(f"transform:{chunk_id}")
                        clean_chunk = self._transform(chunk)
                        self.metrics.record_transformation(len(clean_chunk))
                        self.metrics.end_stage(f"transform:{chunk_id}")

                        # VALIDATE
                        self.metrics.start_stage(f"validate:{chunk_id}")
                        valid = self._validate(clean_chunk, chunk_id)
                        self.metrics.end_stage(f"validate:{chunk_id}")

                        if valid:
                            # LOAD
                            self.metrics.start_stage(f"load:{chunk_id}")
                            rows = self.loader.load(clean_chunk, target_table, if_exists="append")
                            total_loaded += rows
                            self.metrics.record_load(rows)
                            self.metrics.end_stage(f"load:{chunk_id}")
                        else:
                            # Save failed chunks for inspection
                            self._save_failed(chunk, chunk_id)
                            self.metrics.record_failure(len(chunk), "validation_failed")

                    except Exception as e:
                        logger.error(f"Error processing {chunk_id}: {e}")
                        failed_chunks.append(chunk_id)
                        self._save_failed(chunk, chunk_id)
                        self.metrics.record_failure(len(chunk), str(e))

                    progress.advance(task)

            # ── STEP 3: FINAL REPORT ──────────────────────
            self.metrics.print_summary()

            console.print(Panel.fit(
                f"[bold green]✅ Pipeline Complete![/]\n"
                f"Total rows loaded: [yellow]{total_loaded:,}[/]\n"
                f"Failed batches: [red]{len(failed_chunks)}[/]",
                border_style="green"
            ))

            return {"loaded": total_loaded, "failed_batches": len(failed_chunks)}

        except Exception as e:
            logger.critical(f"Pipeline '{self.name}' crashed: {e}", exc_info=True)
            console.print(Panel.fit(f"[bold red]❌ Pipeline FAILED: {e}[/]", border_style="red"))
            raise

    # ══════════════════════════════════════════════════════
    #   PRIVATE HELPERS
    # ══════════════════════════════════════════════════════

    def _extract(self, source: str):
        """
        EXTRACT step.
        Returns a generator of raw DataFrames (one per batch).
        """
        if source == "demo":
            logger.info("Using demo data generator")
            yield from self._generate_demo_data()

        elif source == "csv":
            csv_path = Path(Config.pipeline.raw_data_path) / "input.csv"
            if not csv_path.exists():
                logger.error(f"Input file not found: {csv_path}")
                return
            with CSVExtractor(str(csv_path), batch_size=Config.pipeline.batch_size) as ext:
                for chunk in ext.extract():
                    self.metrics.record_extraction(len(chunk))
                    yield chunk

        elif source == "api":
            with APIExtractor(
                base_url="https://jsonplaceholder.typicode.com",
                endpoint="/posts",
                batch_size=20,
            ) as ext:
                for chunk in ext.extract():
                    self.metrics.record_extraction(len(chunk))
                    yield chunk
        else:
            raise ValueError(f"Unknown source: '{source}'. Use 'demo', 'csv', or 'api'.")

    def _transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        TRANSFORM step.
        Applies a chain of cleaning operations.
        """
        transformer = DataTransformer(df)

        result = (
            transformer
            .clean_column_names()
            .handle_nulls(strategy="fill_median", threshold=0.6)
            .remove_duplicates()
            .standardise_text(
                columns=[c for c in df.columns if df[c].dtype == "object"],
                case="lower",
            )
            .add_derived_columns({
                "_pipeline_name": lambda df: self.name,
                "_processed_at": lambda df: pd.Timestamp.now(),
            })
            .transform()
        )

        return result

    def _validate(self, df: pd.DataFrame, batch_id: str) -> bool:
        """
        VALIDATE step.
        Returns True if data passes quality checks.
        """
        validator = (
            DataValidator(strict_mode=False)   # warn but don't crash
            .min_row_count(1)
        )

        result = validator.validate(df)

        if not result.passed:
            logger.warning(f"Batch '{batch_id}' failed validation: {result.summary()}")
            # Save validation report
            Path("logs").mkdir(exist_ok=True)
            with open(f"logs/validation_{batch_id}.json", "w") as f:
                import json
                json.dump({"errors": result.errors, "warnings": result.warnings, "stats": result.stats}, f, indent=2)

        return result.passed or len(result.errors) == 0

    def _save_failed(self, df: pd.DataFrame, batch_id: str):
        """Save failed data to the 'failed' folder for manual review."""
        path = Path(Config.pipeline.failed_data_path) / f"{batch_id}_failed.csv"
        path.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(path, index=False)
        logger.warning(f"Failed data saved: {path}")

    def _generate_demo_data(self):
        """
        DEMO DATA GENERATOR.

        Creates realistic-looking fake data for testing.
        Uses the `faker` library to generate names, emails, etc.

        Like a movie prop department creating fake money for a film —
        it looks real but is safe to use.
        """
        from faker import Faker
        import numpy as np
        import random

        fake = Faker()
        Faker.seed(42)      # seed for reproducibility (same data each run)
        np.random.seed(42)

        batch_size = Config.pipeline.batch_size
        num_batches = 3

        logger.info(f"Generating {num_batches} demo batches of {batch_size} rows each")

        for batch_num in range(num_batches):
            n = batch_size
            rows = []
            for _ in range(n):
                rows.append({
                    "employee_id": fake.unique.random_int(min=1000, max=99999),
                    "first_name":  fake.first_name(),
                    "last_name":   fake.last_name(),
                    "email":       fake.email(),
                    "department":  random.choice(["Engineering", "Sales", "HR", "Finance", "Marketing"]),
                    "salary":      round(random.gauss(75000, 20000), 2),
                    "hire_date":   fake.date_between(start_date="-10y", end_date="today").isoformat(),
                    "country":     fake.country_code(),
                    "age":         random.randint(18, 65),
                    "performance": random.choice(["excellent", "good", "average", "poor"]),
                    # Intentionally inject some data quality issues for demo:
                    "phone":       fake.phone_number() if random.random() > 0.1 else None,  # 10% null
                    "manager_id":  random.randint(100, 999) if random.random() > 0.05 else None,
                })

            # Add some extreme outliers to test outlier handling
            rows[0]["salary"] = 999999.99   # impossibly high
            rows[1]["age"] = -5             # invalid age
            rows[2]["salary"] = None        # missing value

            df = pd.DataFrame(rows)
            self.metrics.record_extraction(len(df))
            logger.debug(f"Generated demo batch {batch_num + 1}: {len(df)} rows")
            yield df

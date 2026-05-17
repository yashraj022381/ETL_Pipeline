"""
=============================================================
  MONITORING / LOGGER.PY  —  The Pipeline's Diary
=============================================================

WHAT IS LOGGING?
  Imagine you're baking a cake and you write down every step:
    "10:00 — mixed flour"
    "10:05 — added eggs"
    "10:10 — ERROR: forgot sugar!"

  That's logging. Our pipeline writes down everything that
  happens so we can go back and figure out what went wrong.

LOG LEVELS (from calmest to most urgent):
  DEBUG   → "I'm in function X right now" (very detailed)
  INFO    → "Step 3 completed successfully"
  WARNING → "Something looks odd but I'll keep going"
  ERROR   → "Something broke! Trying to recover..."
  CRITICAL→ "Everything is on fire. Please help."
=============================================================
"""

import json
import time
from datetime import datetime
from pathlib import Path
from loguru import logger
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.progress import Progress, SpinnerColumn, BarColumn, TextColumn, TimeElapsedColumn
from config.settings import Config

# Rich console — makes our terminal output pretty with colours
console = Console()


def setup_logger():
    """
    SETUP THE DIARY.

    This configures WHERE logs go (screen + file) and
    WHAT FORMAT they use.

    Like setting up a journal: choosing the notebook and
    the pen colour.
    """
    # Make sure the logs folder exists
    Path("logs").mkdir(exist_ok=True)

    # Remove any default loguru handlers
    logger.remove()

    # Handler 1: Write to the screen
    logger.add(
        lambda msg: console.print(msg, end=""),
        format="<green>{time:HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan> - <level>{message}</level>",
        level=Config.monitoring.log_level,
        colorize=True,
    )

    # Handler 2: Write to a file (keeps history even after restart)
    logger.add(
        Config.monitoring.log_file,
        format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {name}:{function}:{line} - {message}",
        level="DEBUG",            # capture everything in the file
        rotation="10 MB",         # start a new file when this one hits 10 MB
        retention="7 days",       # delete files older than 7 days
        compression="zip",        # zip old logs to save space
        enqueue=True,             # thread-safe writing
    )

    return logger


# ── METRICS COLLECTOR ──────────────────────────────────────
class MetricsCollector:
    """
    THE SCOREBOARD.

    Keeps track of numbers that tell us how well the
    pipeline is performing:
      - How many rows processed?
      - How many errors?
      - How long did each step take?

    Like a sports scoreboard, but for data.
    """

    def __init__(self, pipeline_name: str):
        self.pipeline_name = pipeline_name
        self.start_time = time.time()
        self.metrics = {
            "pipeline": pipeline_name,
            "started_at": datetime.now().isoformat(),
            "rows_extracted": 0,
            "rows_transformed": 0,
            "rows_loaded": 0,
            "rows_failed": 0,
            "errors": [],
            "warnings": [],
            "stage_durations": {},   # how long each stage took (seconds)
            "batches_processed": 0,
        }
        self._stage_start = {}

    def start_stage(self, stage: str):
        """Mark when a stage begins — like hitting a stopwatch."""
        self._stage_start[stage] = time.time()
        logger.info(f"▶  Stage [{stage}] started")

    def end_stage(self, stage: str):
        """Mark when a stage ends — stop the stopwatch."""
        if stage in self._stage_start:
            duration = round(time.time() - self._stage_start[stage], 2)
            self.metrics["stage_durations"][stage] = duration
            logger.info(f"✅ Stage [{stage}] completed in {duration}s")

    def record_extraction(self, rows: int):
        self.metrics["rows_extracted"] += rows
        logger.debug(f"Extracted {rows} rows (total: {self.metrics['rows_extracted']})")

    def record_transformation(self, rows: int):
        self.metrics["rows_transformed"] += rows

    def record_load(self, rows: int):
        self.metrics["rows_loaded"] += rows

    def record_failure(self, rows: int, reason: str):
        self.metrics["rows_failed"] += rows
        self.metrics["errors"].append({"reason": reason, "count": rows, "at": datetime.now().isoformat()})

    def record_warning(self, message: str):
        self.metrics["warnings"].append({"message": message, "at": datetime.now().isoformat()})

    def save(self):
        """Write the metrics to a JSON file — like saving your high score."""
        self.metrics["finished_at"] = datetime.now().isoformat()
        self.metrics["total_duration_seconds"] = round(time.time() - self.start_time, 2)

        Path("logs").mkdir(exist_ok=True)
        with open(Config.monitoring.metrics_file, "w") as f:
            json.dump(self.metrics, f, indent=2)

        return self.metrics

    def print_summary(self):
        """
        PRINT A NICE REPORT to the screen.

        Like getting your report card at the end of school.
        """
        metrics = self.save()

        # Build a colourful table with Rich
        table = Table(title=f"📊 Pipeline Report: {self.pipeline_name}", show_header=True, header_style="bold magenta")
        table.add_column("Metric", style="cyan", width=30)
        table.add_column("Value", style="green")

        table.add_row("Rows Extracted",    str(metrics["rows_extracted"]))
        table.add_row("Rows Transformed",  str(metrics["rows_transformed"]))
        table.add_row("Rows Loaded",       str(metrics["rows_loaded"]))
        table.add_row("Rows Failed",       str(metrics["rows_failed"]) if metrics["rows_failed"] else "0 ✅")
        table.add_row("Total Duration",    f"{metrics['total_duration_seconds']}s")
        table.add_row("Errors",            str(len(metrics["errors"])))
        table.add_row("Warnings",          str(len(metrics["warnings"])))

        console.print(table)

        # Stage-by-stage timing
        if metrics["stage_durations"]:
            timing_table = Table(title="⏱  Stage Timings", header_style="bold blue")
            timing_table.add_column("Stage", style="cyan")
            timing_table.add_column("Duration (s)", style="yellow")
            for stage, dur in metrics["stage_durations"].items():
                timing_table.add_row(stage, str(dur))
            console.print(timing_table)


# ── PROGRESS BAR HELPER ────────────────────────────────────
def get_progress_bar():
    """
    Returns a nice animated progress bar.
    Like a loading screen in a video game.
    """
    return Progress(
        SpinnerColumn(),
        TextColumn("[bold blue]{task.description}"),
        BarColumn(),
        TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
        TimeElapsedColumn(),
        console=console,
    )


# Initialise the logger when this module is imported
setup_logger()

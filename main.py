"""
=============================================================
  MAIN.PY  —  The Front Door
=============================================================

This is the ENTRY POINT — the first file you run.
Like pressing the "Start" button on a machine.

Run modes:
  python main.py demo       — run pipeline with demo data
  python main.py csv        — run pipeline from CSV file
  python main.py api        — run pipeline from REST API
  python main.py schedule   — run pipeline on a schedule
  python main.py test       — run all tests
  python main.py generate   — generate sample CSV data
=============================================================
"""

import sys
import subprocess
from pathlib import Path
from rich.console import Console
from rich.panel import Panel
from rich.table import Table

console = Console()


def print_banner():
    console.print(Panel.fit(
        "[bold cyan]⚡ Advanced ETL Pipeline System[/]\n"
        "[dim]Extract → Transform → Load → Repeat[/]\n"
        "[dim]Version 2.0.0 | Python + Pandas + SQLAlchemy[/]",
        border_style="cyan",
        padding=(1, 4),
    ))


def show_help():
    table = Table(title="Available Commands", header_style="bold magenta")
    table.add_column("Command",     style="cyan", width=20)
    table.add_column("Description", style="white")

    table.add_row("demo",     "Run pipeline with auto-generated fake data")
    table.add_row("csv",      "Run pipeline from data/raw/input.csv")
    table.add_row("api",      "Run pipeline from JSONPlaceholder REST API")
    table.add_row("schedule", "Run pipeline automatically every 60 seconds")
    table.add_row("generate", "Generate a sample 5,000-row CSV for testing")
    table.add_row("test",     "Run all unit + integration tests")
    table.add_row("help",     "Show this help message")

    console.print(table)


def run_demo():
    """Run the full pipeline with generated demo data."""
    from pipeline import ETLPipeline
    pipeline = ETLPipeline("demo_pipeline")
    result = pipeline.run(source="demo", target_table="employees")
    return result


def run_csv():
    """Run the full pipeline from a CSV file."""
    csv_path = Path("data/raw/input.csv")
    if not csv_path.exists():
        console.print(f"[yellow]⚠  No CSV found at {csv_path}[/]")
        console.print("[dim]Run 'python main.py generate' to create a sample file.[/]")
        return

    from pipeline import ETLPipeline
    pipeline = ETLPipeline("csv_pipeline")
    result = pipeline.run(source="csv", target_table="employees_from_csv")
    return result


def run_api():
    """Run the full pipeline from a REST API."""
    from pipeline import ETLPipeline
    pipeline = ETLPipeline("api_pipeline")
    result = pipeline.run(source="api", target_table="posts_from_api")
    return result


def run_scheduled():
    """Run the pipeline on a repeating schedule."""
    from schedulers.pipeline_scheduler import PipelineScheduler
    from pipeline import ETLPipeline

    scheduler = PipelineScheduler()

    def scheduled_run():
        ETLPipeline("scheduled_run").run(source="demo")

    scheduler.add_job(scheduled_run, schedule_type="minutes", every_minutes=1, job_name="demo_pipeline")

    console.print(Panel.fit(
        "[bold yellow]⏰ Scheduler Running[/]\n"
        "Pipeline will run every 60 seconds.\n"
        "Press [bold]Ctrl+C[/] to stop.",
        border_style="yellow"
    ))

    scheduler.start(blocking=True)


def generate_sample_csv():
    """Generate a realistic sample CSV file for testing."""
    from faker import Faker
    import pandas as pd
    import random

    console.print("[cyan]Generating 5,000 row sample CSV...[/]")

    fake = Faker()
    Faker.seed(0)
    random.seed(0)

    rows = []
    for _ in range(5000):
        rows.append({
            "employee_id": fake.unique.random_int(1000, 99999),
            "first_name":  fake.first_name(),
            "last_name":   fake.last_name(),
            "email":       fake.email(),
            "department":  random.choice(["Engineering", "Sales", "HR", "Finance", "Marketing"]),
            "salary":      round(random.gauss(75000, 20000), 2),
            "hire_date":   fake.date_between(start_date="-10y", end_date="today").isoformat(),
            "country":     fake.country_code(),
            "age":         random.randint(18, 65),
            "performance": random.choice(["excellent", "good", "average", "poor"]),
            "phone":       fake.phone_number() if random.random() > 0.1 else None,
        })

    df = pd.DataFrame(rows)
    out_path = Path("data/raw/input.csv")
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out_path, index=False)
    console.print(f"[green]✅ Generated {len(df):,} rows → {out_path}[/]")


def run_tests():
    """Run all tests with pytest."""
    console.print("[cyan]Running test suite...[/]\n")
    result = subprocess.run(
        [sys.executable, "-m", "pytest", "tests/", "-v", "--tb=short", "--no-header"],
        cwd=Path(__file__).parent,
    )
    if result.returncode == 0:
        console.print("\n[bold green]✅ All tests passed![/]")
    else:
        console.print("\n[bold red]❌ Some tests failed. Check output above.[/]")


# ── DISPATCH TABLE ────────────────────────────────────────
COMMANDS = {
    "demo":     run_demo,
    "csv":      run_csv,
    "api":      run_api,
    "schedule": run_scheduled,
    "generate": generate_sample_csv,
    "test":     run_tests,
    "help":     show_help,
}


if __name__ == "__main__":
    print_banner()

    command = sys.argv[1] if len(sys.argv) > 1 else "help"

    if command in COMMANDS:
        COMMANDS[command]()
    else:
        console.print(f"[red]Unknown command: '{command}'[/]")
        show_help()

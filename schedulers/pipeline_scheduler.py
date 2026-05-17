"""
=============================================================
  SCHEDULERS / PIPELINE_SCHEDULER.PY  —  The Alarm Clock
=============================================================

WHAT IS SCHEDULING?
  Real ETL pipelines don't run once manually.
  They run AUTOMATICALLY on a schedule:
    - "Run every night at 2 AM"
    - "Run every Monday at 6 AM"
    - "Run every 15 minutes"

  This is like setting an alarm clock for your pipeline.

  WHY NOT DO THIS MANUALLY?
    - Humans forget
    - Humans sleep (pipelines shouldn't)
    - Consistent timing = consistent data freshness

WHAT TOOLS DO WE USE?
  schedule  : a simple Python library (like a basic alarm clock)
  Celery    : a professional task queue with Redis as the message board
              (used by companies like Instagram and Pinterest)

HOW CELERY WORKS:
  1. You add a task to the queue (like leaving a sticky note)
  2. A "worker" process picks it up and runs it
  3. Results are stored back in Redis
  This lets you run tasks in parallel across many machines!
=============================================================
"""

import time
import threading
from datetime import datetime
from typing import Callable, Optional
import schedule
from loguru import logger


# ── SIMPLE SCHEDULER (no external dependencies) ──────────
class PipelineScheduler:
    """
    A simple scheduler using the `schedule` library.

    Perfect for single-machine deployments.
    Think of it as the alarm clock on your bedside table.
    """

    def __init__(self):
        self._jobs = []
        self._running = False
        self._thread: Optional[threading.Thread] = None

    def add_job(
        self,
        func: Callable,
        schedule_type: str = "daily",
        at_time: str = "02:00",
        every_minutes: Optional[int] = None,
        job_name: str = "unnamed_job",
    ) -> "PipelineScheduler":
        """
        Register a function to run on a schedule.

        schedule_type options:
          "daily"    — run once per day at `at_time`
          "hourly"   — run every hour
          "minutes"  — run every `every_minutes` minutes
          "monday"   — run every Monday (also tuesday, wednesday, etc.)

        Examples:
            scheduler.add_job(run_sales_pipeline, "daily", "03:00")
            scheduler.add_job(sync_users, "minutes", every_minutes=15)
        """
        # Wrap the function to add logging and error handling
        def safe_run():
            logger.info(f"⏰ Scheduled job starting: '{job_name}'")
            start = time.time()
            try:
                func()
                duration = round(time.time() - start, 2)
                logger.info(f"✅ Job '{job_name}' completed in {duration}s")
            except Exception as e:
                logger.error(f"❌ Job '{job_name}' failed: {e}", exc_info=True)

        # Register with the schedule library
        if schedule_type == "daily":
            job = schedule.every().day.at(at_time).do(safe_run)
        elif schedule_type == "hourly":
            job = schedule.every().hour.do(safe_run)
        elif schedule_type == "minutes" and every_minutes:
            job = schedule.every(every_minutes).minutes.do(safe_run)
        elif schedule_type in ("monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"):
            job = getattr(schedule.every(), schedule_type).at(at_time).do(safe_run)
        else:
            raise ValueError(f"Unknown schedule_type: {schedule_type}")

        self._jobs.append({"name": job_name, "type": schedule_type, "job": job})
        logger.info(f"Registered job '{job_name}' — schedule: {schedule_type} {at_time or ''}")
        return self

    def run_now(self, func: Callable, job_name: str = "manual_run"):
        """
        Run a pipeline immediately (without waiting for the schedule).
        Useful for testing or manual triggers.
        """
        logger.info(f"🚀 Manual trigger: '{job_name}'")
        try:
            func()
            logger.info(f"✅ Manual run '{job_name}' complete")
        except Exception as e:
            logger.error(f"❌ Manual run '{job_name}' failed: {e}")
            raise

    def start(self, blocking: bool = True):
        """
        Start the scheduler loop.

        blocking=True  : the program stops here and just runs schedules forever
                         (good for a dedicated scheduler service)
        blocking=False : runs in a background thread
                         (good when you also want to do other things)
        """
        self._running = True
        logger.info(f"Scheduler started. {len(self._jobs)} jobs registered.")
        self._print_schedule()

        def _loop():
            while self._running:
                schedule.run_pending()   # check: is any job due to run?
                time.sleep(1)            # check every second

        if blocking:
            try:
                _loop()
            except KeyboardInterrupt:
                logger.info("Scheduler stopped by user (Ctrl+C)")
        else:
            self._thread = threading.Thread(target=_loop, daemon=True)
            self._thread.start()
            logger.info("Scheduler running in background thread")

    def stop(self):
        """Stop the scheduler gracefully."""
        self._running = False
        schedule.clear()
        logger.info("Scheduler stopped. All jobs cleared.")

    def _print_schedule(self):
        """Print a summary of all registered jobs."""
        logger.info("── SCHEDULED JOBS ──────────────────")
        for j in self._jobs:
            logger.info(f"  • {j['name']} [{j['type']}]")
        logger.info("────────────────────────────────────")


# ── CELERY TASK QUEUE (production-grade) ─────────────────
# This section sets up Celery — the industrial-strength
# task queue used in production environments.
#
# Think of Celery as a post office:
#   - You drop off a letter (task) at the post office (queue)
#   - A postal worker (worker) picks it up and delivers it
#   - You can drop off many letters; they're all processed in parallel
#
# NOTE: Celery requires Redis to be running.
#       For the demo we skip the actual import to avoid errors
#       if Redis isn't installed.

try:
    from celery import Celery
    from celery.schedules import crontab
    from config.settings import Config

    # Create the Celery application
    celery_app = Celery(
        "etl_pipeline",
        broker=Config.redis.url,    # where tasks are sent (Redis)
        backend=Config.redis.url,   # where results are stored (Redis)
    )

    # Configure Celery
    celery_app.conf.update(
        task_serializer="json",
        accept_content=["json"],
        result_serializer="json",
        timezone="UTC",
        enable_utc=True,
        task_track_started=True,
        task_acks_late=True,    # only mark as done AFTER success (important for reliability)
        worker_prefetch_multiplier=1,  # one task at a time per worker (fair scheduling)
    )

    # ── BEAT SCHEDULE (Celery's cron) ──────────────────────
    # This is Celery Beat — the part that triggers tasks on a schedule,
    # like a cron job but more powerful.
    celery_app.conf.beat_schedule = {
        "daily-sales-etl": {
            "task": "schedulers.pipeline_scheduler.run_sales_pipeline",
            "schedule": crontab(hour=2, minute=0),   # 2:00 AM daily
        },
        "hourly-metrics": {
            "task": "schedulers.pipeline_scheduler.run_metrics_pipeline",
            "schedule": crontab(minute=0),            # top of every hour
        },
    }

    # ── CELERY TASKS ───────────────────────────────────────
    @celery_app.task(
        bind=True,
        max_retries=3,
        default_retry_delay=60,   # wait 60s between retries
        name="schedulers.pipeline_scheduler.run_sales_pipeline",
    )
    def run_sales_pipeline_task(self):
        """
        A Celery task for the sales pipeline.

        `bind=True` means `self` is the task instance — we can
        call `self.retry()` to retry after a failure.
        """
        try:
            logger.info("Celery task: run_sales_pipeline started")
            # Import here to avoid circular imports
            from pipeline import ETLPipeline
            pipeline = ETLPipeline("celery_sales_run")
            pipeline.run()
        except Exception as exc:
            logger.error(f"Celery task failed: {exc}")
            raise self.retry(exc=exc)

    CELERY_AVAILABLE = True
    logger.debug("Celery configured successfully")

except ImportError:
    CELERY_AVAILABLE = False
    logger.debug("Celery not available — using simple scheduler only")

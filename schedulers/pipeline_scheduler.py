# =============================================================
#  SCHEDULERS/PIPELINE_SCHEDULER.PY
#  
#  CELERY = A task queue system
#
#  Think of it like a RESTAURANT:
#    - You (the boss) write orders on tickets  → tasks
#    - The kitchen (workers) pick up tickets   → celery workers  
#    - The ticket board                        → Redis
#    - The timer that creates orders           → Celery Beat
# =============================================================

import os
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from celery import Celery
from celery.schedules import crontab
from loguru import logger

# ── CREATE THE CELERY APP ─────────────────────────────────────
# broker  = where tasks are SENT     (Redis is the message board)
# backend = where RESULTS are stored (Redis stores the answers)

celery_app = Celery(
    "etl_pipeline",                    # name of our app
    broker="redis://localhost:6379/0", # send tasks here
    backend="redis://localhost:6379/0" # store results here
)

# ── SETTINGS ─────────────────────────────────────────────────
celery_app.conf.update(
    task_serializer="json",      # tasks travel as JSON text
    result_serializer="json",    # results stored as JSON
    timezone="Asia/Kolkata",     # YOUR timezone IST
    enable_utc=False,
    task_track_started=True,     # show "STARTED" status
    task_acks_late=True,         # only mark done AFTER success
)

# ── SCHEDULED TASKS (like cron jobs) ─────────────────────────
# This is Celery Beat — the alarm clock that creates tasks
# crontab(hour=2, minute=0) = run at 2:00 AM every day

celery_app.conf.beat_schedule = {

    # Run demo pipeline every day at 2 AM
    "daily-demo-pipeline": {
        "task": "schedulers.pipeline_scheduler.run_demo_task",
        "schedule": crontab(hour=2, minute=0),
        "options": {"queue": "pipeline"}
    },

    # Run every 5 minutes (for testing)
    "every-5-minutes": {
        "task": "schedulers.pipeline_scheduler.run_demo_task",
        "schedule": crontab(minute="*/5"),
        "options": {"queue": "pipeline"}
    },
}

# ── DEFINE TASKS ─────────────────────────────────────────────
# @celery_app.task turns a normal function into a Celery task
# bind=True  gives us "self" so we can retry on failure
# max_retries=3 means try 3 times before giving up

@celery_app.task(
    bind=True,
    max_retries=3,
    default_retry_delay=30,    # wait 30 seconds between retries
    name="schedulers.pipeline_scheduler.run_demo_task",
    queue="pipeline",
)
def run_demo_task(self):
    """
    CELERY TASK: Run the demo pipeline.
    
    This function runs in a SEPARATE WORKER PROCESS.
    Like a kitchen worker picking up an order ticket.
    """
    try:
        logger.info(f"Celery task started | ID: {self.request.id}")
        
        from pipeline import ETLPipeline
        pipeline = ETLPipeline(f"celery_run_{self.request.id[:8]}")
        result = pipeline.run(source="demo", target_table="celery_employees")
        
        logger.info(f"Celery task complete: {result}")
        return result   # stored in Redis for retrieval

    except Exception as exc:
        logger.error(f"Task failed: {exc}")
        # retry() waits 30s then tries again, up to 3 times
        raise self.retry(exc=exc)


@celery_app.task(
    bind=True,
    name="schedulers.pipeline_scheduler.run_csv_task",
    queue="pipeline",
)
def run_csv_task(self):
    """CELERY TASK: Run the CSV pipeline."""
    try:
        from pipeline import ETLPipeline
        pipeline = ETLPipeline("celery_csv")
        result = pipeline.run(source="csv", target_table="celery_csv_data")
        return result
    except Exception as exc:
        raise self.retry(exc=exc)


# ── SIMPLE SCHEDULER (no Redis needed) ───────────────────────
# Use this for testing without Redis

import schedule
import time
import threading
from typing import Callable, Optional

class SimpleScheduler:
    """
    Basic scheduler using the 'schedule' library.
    No Redis needed — perfect for learning and testing.
    """

    def __init__(self):
        self._running = False
        self._thread: Optional[threading.Thread] = None

    def add_daily_job(self, func: Callable, at_time: str, name: str):
        """Run a function every day at a specific time."""
        def safe_run():
            logger.info(f"⏰ Running scheduled job: {name}")
            try:
                func()
                logger.info(f"✅ Job '{name}' complete")
            except Exception as e:
                logger.error(f"❌ Job '{name}' failed: {e}")

        schedule.every().day.at(at_time).do(safe_run)
        logger.info(f"Scheduled: '{name}' runs daily at {at_time}")

    def add_interval_job(self, func: Callable, minutes: int, name: str):
        """Run a function every N minutes."""
        def safe_run():
            logger.info(f"⏰ Interval job: {name}")
            try:
                func()
            except Exception as e:
                logger.error(f"❌ {name} failed: {e}")

        schedule.every(minutes).minutes.do(safe_run)
        logger.info(f"Scheduled: '{name}' runs every {minutes} minutes")

    def start_background(self):
        """Run scheduler in background thread — doesn't block your program."""
        self._running = True

        def loop():
            while self._running:
                schedule.run_pending()  # check: is any job due?
                time.sleep(1)           # check every second

        self._thread = threading.Thread(target=loop, daemon=True)
        self._thread.start()
        logger.info("Scheduler running in background")

    def stop(self):
        self._running = False
        schedule.clear()
        logger.info("Scheduler stopped")

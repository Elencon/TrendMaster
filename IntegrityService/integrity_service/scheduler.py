from apscheduler.schedulers.background import BackgroundScheduler
from .integrity_runner import run_integrity_and_backup
from .config import load_config

scheduler = BackgroundScheduler()


def start_scheduler():
    cfg = load_config()
    schedule = cfg["schedule"]  # e.g. {"day_of_week": "sun", "hour": 2}

    scheduler.add_job(
        run_integrity_and_backup,
        trigger="cron",
        **schedule
    )

    scheduler.start()
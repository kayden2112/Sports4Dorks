from apscheduler.schedulers.blocking import BlockingScheduler
from optimize_totals import get_game_totals, compute_totals_value, persist_value_totals
from api_service.config import settings
from datetime import datetime

def job():
    df = get_game_totals()
    vt = compute_totals_value(df)
    persist_value_totals(vt)

if __name__ == "__main__":
    sched = BlockingScheduler()
    sched.add_job(
        job,
        'interval',
        seconds=settings.ETL_INTERVAL_SECONDS,
        next_run_time=datetime.now()
    )
    sched.start()

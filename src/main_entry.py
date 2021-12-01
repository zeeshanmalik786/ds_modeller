from job1.ium_aggregations import ium
from pytz import timezone
from datetime import datetime, timedelta

def main(run_date):

    print("Current Date of Run", run_date)

    job_0 = ium()
    status = job_0.__ium_aggregations__(run_date)

    if status is True:
        print("Job 1 Done")
    else:
        print("Job 1 crashed")

if __name__ == '__main__':

    fmt = "%Y-%m-%d"
    tz = timezone('EST')
    current_date = datetime.now(tz).strftime(fmt)
    run_date = datetime.strptime(current_date, "%Y-%m-%d") + timedelta(days=-1)
    main(run_date)
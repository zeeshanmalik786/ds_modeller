from IPTV.iptv_monthly_downstream import IPTV


def main():

    job_1 = IPTV()
    status = job_1.__iptv_forecast_calculation__()

    if status == True:

        print("IPTV Forecast Job Successfully Completed")
    else:
        print("IPTV Forecast Job Crashed")

    status = job_1.__iptv_approximations__()

    if status == True:

        print("IPTV Approximation Job Successfully Completed")
    else:
        print("IPTV Approximation Job Crashed")


def run():

    main()
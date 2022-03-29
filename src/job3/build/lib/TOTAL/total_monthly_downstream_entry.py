from TOTAL.total_monthly_downstream import TOTAL


def main():

    job_1 = TOTAL()
    status = job_1.__total_approximations__()

    if status == True:

        print("Total Approximations Job Successfully Completed")
    else:
        print("Total Approximations Forecast Job Crashed")


def run():

    main()
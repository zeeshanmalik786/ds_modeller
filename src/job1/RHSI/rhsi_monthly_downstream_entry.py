from RHSI.rhsi_monthly_downstream import RHSI


def main():

    job_2 = RHSI()
    status = job_2.__rhsi_calculation__()

    if status == True:

        print("RHSI Job Successfully Completed")
    else:
        print("RHSI Job Crashed")

def run():

    main()
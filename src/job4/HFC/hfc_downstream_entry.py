from HFC.hfc_downstream import HFC_DOWNSTREAM


def main():

    job_1 = HFC_DOWNSTREAM()
    status = job_1.__hfc_downstream_approximations__()

    if status == True:

        print("Total Approximations Job Successfully Completed")
    else:
        print("Total Approximations Forecast Job Crashed")


def run():

    main()
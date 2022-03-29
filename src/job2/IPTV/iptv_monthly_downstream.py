from IPTV.config import config
from IPTV.preprocess import preprocessor
from pyspark.sql.functions import col, array_contains, element_at
from pyspark.sql.types import DoubleType
from pyspark.sql.functions import sum, round, lit, count, max
from pyspark.sql.types import DoubleType
from pyspark.sql.functions import sum, round, lit, count

class IPTV:

    def __init__(self):
        self.con = config()
        self.obj = preprocessor(self.con.context)
        self.spark = self.con.spark


    def __iptv_forecast_calculation__(self):

        total_df = self.obj.get_data("default.ds_modeller_down_byte_daily", ["PHUB",
                                                                             "cmts_host_name",
                                                                             "MAC_DOMAIN",
                                                                             "total_rhsi_only_subs_ium",
                                                                             "IPTV_subs_per_md"]). \
            withColumn("total_rhsi_only_subs_ium", col("total_rhsi_only_subs_ium").cast(DoubleType())). \
            withColumn("IPTV_subs_per_md", col("IPTV_subs_per_md").cast(DoubleType()))

        sum_of_cm_df = total_df.groupBy("PHUB", "cmts_host_name", "MAC_DOMAIN").agg(sum("total_rhsi_only_subs_ium"),
                                                                                    sum("IPTV_subs_per_md")). \
            withColumnRenamed("sum(total_rhsi_only_subs_ium)", "total_rhsi_only_subs_ium"). \
            withColumnRenamed("sum(IPTV_subs_per_md)", "IPTV_subs_per_md"). \
            withColumn("total_subs", round(col("total_rhsi_only_subs_ium") + col("IPTV_subs_per_md"), 0)). \
            drop("total_rhsi_only_subs_ium", "IPTV_subs_per_md")

        total_cm = sum_of_cm_df.groupBy().agg(sum("total_subs")). \
            withColumnRenamed("sum(total_subs)", "denom").collect()[0][0]

        print(total_cm)

        sum_of_cm_df = sum_of_cm_df.withColumn("denom", lit(total_cm)). \
            withColumn("total_subs_per_network", round((col("total_subs") / col("denom")) * 100, 3)). \
            drop("total_subs", "denom")

        subscriber_forecast = self.obj.get_data("default.ds_modeller_subscriber_forecast",
                                       ["forecast_date", "forecast_subscriber"]).orderBy(col("forecast_date"))
        subscriber_forecast.show()
        a = subscriber_forecast.collect()

        print(subscriber_forecast.select("forecast_subscriber").first())
        print(a[1][1])
        print(a[2][1])
        print(a[3][1])
        print(a[4][1])

        sum_of_cm_df = sum_of_cm_df.withColumn("add_sub_per_phub_1", round(
            (col("total_subs_per_network") * subscriber_forecast.select("forecast_subscriber").first()[0]) / 100, 0)). \
            withColumn("add_sub_per_phub_2", round((col("total_subs_per_network") * a[1][1]) / 100, 0)). \
            withColumn("add_sub_per_phub_3", round((col("total_subs_per_network") * a[2][1]) / 100, 0)). \
            withColumn("add_sub_per_phub_4", round((col("total_subs_per_network") * a[3][1]) / 100, 0)). \
            withColumn("add_sub_per_phub_5", round((col("total_subs_per_network") * a[4][1]) / 100, 0)). \
            withColumn("add_sub_per_phub_6", round((col("total_subs_per_network") * a[5][1]) / 100, 0)). \
            withColumn("add_sub_per_phub_7", round((col("total_subs_per_network") * a[6][1]) / 100, 0)). \
            withColumn("add_sub_per_phub_8", round((col("total_subs_per_network") * a[7][1]) / 100, 0)). \
            withColumn("add_sub_per_phub_9", round((col("total_subs_per_network") * a[8][1]) / 100, 0)). \
            withColumn("add_sub_per_phub_10", round((col("total_subs_per_network") * a[9][1]) / 100, 0)). \
            withColumn("add_sub_per_phub_11", round((col("total_subs_per_network") * a[10][1]) / 100, 0)). \
            withColumn("add_sub_per_phub_12", round((col("total_subs_per_network") * a[-1][-1]) / 100, 0)). \
            filter(col("PHUB") != "N/A")

        utilization_per_phub = self.obj.get_data("default.ds_modeller_down_byte_daily_RHSI_Approximation", ["PHUB",
                                                                                                   "cmts_host_name",
                                                                                                   "MAC_DOMAIN",
                                                                                                   "iptv_throughput_by_phub"]). \
            withColumnRenamed("iptv_throughput_by_phub", "utilization_per_phub")

        sum_of_cm_df_with_util = self.obj.join_two_frames(sum_of_cm_df, utilization_per_phub, "inner",
                                                 ["PHUB", "cmts_host_name", "MAC_DOMAIN"])
        # withColumn("utilization_per_phub", func.when(col("utilization_per_phub").cast(DoubleType())>=7.0, 7.17).otherwise(col("utilization_per_phub")))

        sum_of_cm_df_with_util = sum_of_cm_df_with_util.withColumn("add_mbps_approx_month_1", round(
            col("add_sub_per_phub_1") * col("utilization_per_phub"), 2)). \
            withColumn("add_mbps_approx_month_2", round(col("add_sub_per_phub_2") * col("utilization_per_phub"), 2)). \
            withColumn("add_mbps_approx_month_3", round(col("add_sub_per_phub_3") * col("utilization_per_phub"), 2)). \
            withColumn("add_mbps_approx_month_4", round(col("add_sub_per_phub_4") * col("utilization_per_phub"), 2)). \
            withColumn("add_mbps_approx_month_5", round(col("add_sub_per_phub_5") * col("utilization_per_phub"), 2)). \
            withColumn("add_mbps_approx_month_6", round(col("add_sub_per_phub_6") * col("utilization_per_phub"), 2)). \
            withColumn("add_mbps_approx_month_7", round(col("add_sub_per_phub_7") * col("utilization_per_phub"), 2)). \
            withColumn("add_mbps_approx_month_8", round(col("add_sub_per_phub_8") * col("utilization_per_phub"), 2)). \
            withColumn("add_mbps_approx_month_9", round(col("add_sub_per_phub_9") * col("utilization_per_phub"), 2)). \
            withColumn("add_mbps_approx_month_10", round(col("add_sub_per_phub_10") * col("utilization_per_phub"), 2)). \
            withColumn("add_mbps_approx_month_11", round(col("add_sub_per_phub_11") * col("utilization_per_phub"), 2)). \
            withColumn("add_mbps_approx_month_12", round(col("add_sub_per_phub_12") * col("utilization_per_phub"), 2)). \
            withColumn("utilization_per_phub", col("utilization_per_phub").cast(DoubleType()))

        self.spark.sql("DROP TABLE IF EXISTS default.ds_modeller_ignite_tv_forecast")
        sum_of_cm_df_with_util.write.saveAsTable("default.ds_modeller_ignite_tv_forecast")


    def __iptv_approximations__(self):


        md_perc_of_site = self.obj.get_data("default.ds_modeller_down_byte_daily", "*"). \
            withColumn("md_iptv_sub_impact_mbps", col("md_iptv_sub_impact_mbps").cast(DoubleType())). \
            withColumn("md_perc_of_site", col("md_perc_of_site").cast(DoubleType()))

        # md_perc_of_site = md_perc_of_site.groupBy("xref", "PHUB").agg(sum(col("md_perc_of_site").cast(DoubleType())),
        #                                                       sum(col("md_iptv_sub_impact_mbps").cast(DoubleType())),
        #                                                       count(col("md_perc_of_site").cast(DoubleType())),
        #                                                       count(col("md_iptv_sub_impact_mbps").cast(DoubleType()))).\
        # withColumn("md_perc_of_site",col("sum(CAST(md_perc_of_site AS DOUBLE))")/col("count(CAST(md_perc_of_site AS DOUBLE))")).\
        # withColumn("md_iptv_sub_impact_mbps",col("sum(CAST(md_iptv_sub_impact_mbps AS DOUBLE))")/col("count(CAST(md_iptv_sub_impact_mbps AS DOUBLE))")).\
        # drop("sum(CAST(md_perc_of_site AS DOUBLE))",
        #     "count(CAST(md_perc_of_site AS DOUBLE))",
        #     "sum(CAST(md_iptv_sub_impact_mbps AS DOUBLE))",
        #     "count(CAST(md_iptv_sub_impact_mbps AS DOUBLE))")

        iptv_forecast = self.obj.get_data("default.ds_modeller_ignite_tv_forecast", ["PHUB",
                                                                            "cmts_host_name",
                                                                            "MAC_DOMAIN",
                                                                            "add_mbps_approx_month_1",
                                                                            "add_mbps_approx_month_2",
                                                                            "add_mbps_approx_month_3",
                                                                            "add_mbps_approx_month_4",
                                                                            "add_mbps_approx_month_5",
                                                                            "add_mbps_approx_month_6",
                                                                            "add_mbps_approx_month_7",
                                                                            "add_mbps_approx_month_8",
                                                                            "add_mbps_approx_month_9",
                                                                            "add_mbps_approx_month_10",
                                                                            "add_mbps_approx_month_11",
                                                                            "add_mbps_approx_month_12"
                                                                            ])

        iptv_md_perc_forecast = self.obj.join_two_frames(md_perc_of_site, iptv_forecast, "inner",
                                                ["PHUB", "cmts_host_name", "MAC_DOMAIN"])

        iptv_approximations = iptv_md_perc_forecast. \
            withColumn("iptv_month_0", col("md_iptv_sub_impact_mbps")). \
            withColumn("iptv_month_1", col("md_iptv_sub_impact_mbps") + (
            round(col("md_perc_of_site") * col("add_mbps_approx_month_1"), 3))). \
            withColumn("iptv_month_2",
                       col("iptv_month_1") + (round(col("md_perc_of_site") * col("add_mbps_approx_month_2"), 3))). \
            withColumn("iptv_month_3",
                       col("iptv_month_2") + (round(col("md_perc_of_site") * col("add_mbps_approx_month_3"), 3))). \
            withColumn("iptv_month_4",
                       col("iptv_month_3") + (round(col("md_perc_of_site") * col("add_mbps_approx_month_4"), 3))). \
            withColumn("iptv_month_5",
                       col("iptv_month_4") + (round(col("md_perc_of_site") * col("add_mbps_approx_month_5"), 3)) + 1). \
            withColumn("iptv_month_6",
                       col("iptv_month_5") + (round(col("md_perc_of_site") * col("add_mbps_approx_month_6"), 3)) + 1). \
            withColumn("iptv_month_7",
                       col("iptv_month_6") + (round(col("md_perc_of_site") * col("add_mbps_approx_month_7"), 3)) + 1). \
            withColumn("iptv_month_8",
                       col("iptv_month_7") + (round(col("md_perc_of_site") * col("add_mbps_approx_month_8"), 3)) + 1). \
            withColumn("iptv_month_9",
                       col("iptv_month_8") + (round(col("md_perc_of_site") * col("add_mbps_approx_month_9"), 3)) + 1). \
            withColumn("iptv_month_10",
                       col("iptv_month_9") + (round(col("md_perc_of_site") * col("add_mbps_approx_month_10"), 3)) + 1). \
            withColumn("iptv_month_11",
                       col("iptv_month_10") + (round(col("md_perc_of_site") * col("add_mbps_approx_month_11"), 3)) + 1). \
            withColumn("iptv_month_12",
                       col("iptv_month_11") + (round(col("md_perc_of_site") * col("add_mbps_approx_month_12"), 3)) + 1). \
            withColumn("iptv_month_13",
                       col("iptv_month_12") + (round(col("md_perc_of_site") * col("add_mbps_approx_month_12"), 3)) + 1). \
            withColumn("iptv_month_14",
                       col("iptv_month_13") + (round(col("md_perc_of_site") * col("add_mbps_approx_month_12"), 3)) + 1). \
            withColumn("iptv_month_15",
                       col("iptv_month_14") + (round(col("md_perc_of_site") * col("add_mbps_approx_month_12"), 3)) + 1). \
            withColumn("iptv_month_16",
                       col("iptv_month_15") + (round(col("md_perc_of_site") * col("add_mbps_approx_month_12"), 3)) + 1). \
            withColumn("iptv_month_17",
                       col("iptv_month_16") + (round(col("md_perc_of_site") * col("add_mbps_approx_month_12"), 3)) + 1). \
            drop("add_mbps_approx_month_1",
                 "add_mbps_approx_month_2",
                 "add_mbps_approx_month_3",
                 "add_mbps_approx_month_4",
                 "add_mbps_approx_month_5",
                 "add_mbps_approx_month_6",
                 "add_mbps_approx_month_7",
                 "add_mbps_approx_month_8",
                 "add_mbps_approx_month_9",
                 "add_mbps_approx_month_10",
                 "add_mbps_approx_month_11",
                 "add_mbps_approx_month_12")

        self.spark.sql("DROP TABLE IF EXISTS default.ds_modeller_down_byte_daily_IPTV_Approximation")
        iptv_approximations.write.saveAsTable("default.ds_modeller_down_byte_daily_IPTV_Approximation")

        return True











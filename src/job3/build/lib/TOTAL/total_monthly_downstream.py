from TOTAL.config import config
from TOTAL.preprocess import preprocessor
from pyspark.sql.functions import col, array_contains, element_at
from pyspark.sql.types import DoubleType
from pyspark.sql.functions import sum, round, lit, count, max
from pyspark.sql.types import DoubleType
from pyspark.sql.functions import sum, round, lit, count

class TOTAL:

    def __init__(self):
        self.con = config()
        self.obj = preprocessor(self.con.context)
        self.spark = self.con.spark



    def __total_approximations__(self):

        rhsi_approximations = self.obj.get_data("default.ds_modeller_down_byte_daily_RHSI_Approximation", "*"). \
            select("xref",
                   "PHUB",
                   "cmts_host_name",
                   "MAC_DOMAIN",
                   "RHSI_Month_0",
                   "RHSI_Month_1",
                   "RHSI_Month_2",
                   "RHSI_Month_3",
                   "RHSI_Month_4",
                   "RHSI_Month_5",
                   "RHSI_Month_6",
                   "RHSI_Month_7",
                   "RHSI_Month_8",
                   "RHSI_Month_9",
                   "RHSI_Month_10",
                   "RHSI_Month_11",
                   "RHSI_Month_12",
                   "RHSI_Month_13",
                   "RHSI_Month_14",
                   "RHSI_Month_15",
                   "RHSI_Month_16",
                   "RHSI_Month_17")

        iptv_approximations = self.obj.get_data("default.ds_modeller_down_byte_daily_IPTV_Approximation", "*")

        join_iptv_rhsi = self.obj.join_two_frames(rhsi_approximations, iptv_approximations, "inner",
                                         ["xref", "PHUB", "cmts_host_name", "MAC_DOMAIN"]). \
            withColumn("total_traffic_month0", col("RHSI_Month_0") + col("iptv_month_0")). \
            withColumn("total_traffic_month1", col("RHSI_Month_1") + col("iptv_month_1")). \
            withColumn("total_traffic_month2", col("RHSI_Month_2") + col("iptv_month_2")). \
            withColumn("total_traffic_month3", col("RHSI_Month_3") + col("iptv_month_3")). \
            withColumn("total_traffic_month4", col("RHSI_Month_4") + col("iptv_month_4")). \
            withColumn("total_traffic_month5", col("RHSI_Month_5") + col("iptv_month_5")). \
            withColumn("total_traffic_month6", col("RHSI_Month_6") + col("iptv_month_6")). \
            withColumn("total_traffic_month7", col("RHSI_Month_7") + col("iptv_month_7")). \
            withColumn("total_traffic_month8", col("RHSI_Month_8") + col("iptv_month_8")). \
            withColumn("total_traffic_month9", col("RHSI_Month_9") + col("iptv_month_9")). \
            withColumn("total_traffic_month10", col("RHSI_Month_10") + col("iptv_month_10")). \
            withColumn("total_traffic_month11", col("RHSI_Month_11") + col("iptv_month_11")). \
            withColumn("total_traffic_month12", col("RHSI_Month_12") + col("iptv_month_12")). \
            withColumn("total_traffic_month13", col("RHSI_Month_13") + col("iptv_month_13")). \
            withColumn("total_traffic_month14", col("RHSI_Month_14") + col("iptv_month_14")). \
            withColumn("total_traffic_month15", col("RHSI_Month_15") + col("iptv_month_15")). \
            withColumn("total_traffic_month16", col("RHSI_Month_16") + col("iptv_month_16")). \
            withColumn("total_traffic_month17", col("RHSI_Month_17") + col("iptv_month_17")). \
            select("xref",
                   "PHUB",
                   "regional_rhsi_cmgr",
                   "regional_t5_trigger",
                   "regional_t3_4_trigger",
                   "CBU",
                   "eng_region",
                   "cmts_name",
                   "mac_domain",
                   "total_ds_capacity_in_mbps_scqam_ofdm",
                   "current_md_util_mbps",
                   "current_ct_fwds_in_md",
                   "total_rhsi_only_subs_ium",
                   "iptv_throughput_by_phub",
                   "md_perc_of_site",
                   "md_iptv_sub_impact_mbps",
                   "iptv_perc_of_total_cap",
                   "current_md_rhsi_only_mbps",
                   "current_perc_util_per_ium",
                   "RHSI_Month_0",
                   "RHSI_Month_1",
                   "RHSI_Month_2",
                   "RHSI_Month_3",
                   "RHSI_Month_4",
                   "RHSI_Month_5",
                   "RHSI_Month_6",
                   "RHSI_Month_7",
                   "RHSI_Month_8",
                   "RHSI_Month_9",
                   "RHSI_Month_10",
                   "RHSI_Month_11",
                   "RHSI_Month_12",
                   "RHSI_Month_13",
                   "RHSI_Month_14",
                   "RHSI_Month_15",
                   "RHSI_Month_16",
                   "RHSI_Month_17",
                   "iptv_month_0",
                   "iptv_month_1",
                   "iptv_month_2",
                   "iptv_month_3",
                   "iptv_month_4",
                   "iptv_month_5",
                   "iptv_month_6",
                   "iptv_month_7",
                   "iptv_month_8",
                   "iptv_month_9",
                   "iptv_month_10",
                   "iptv_month_11",
                   "iptv_month_12",
                   "iptv_month_13",
                   "iptv_month_14",
                   "iptv_month_15",
                   "iptv_month_16",
                   "iptv_month_17",
                   "total_traffic_month0",
                   "total_traffic_month1",
                   "total_traffic_month2",
                   "total_traffic_month3",
                   "total_traffic_month4",
                   "total_traffic_month5",
                   "total_traffic_month6",
                   "total_traffic_month7",
                   "total_traffic_month8",
                   "total_traffic_month9",
                   "total_traffic_month10",
                   "total_traffic_month11",
                   "total_traffic_month12",
                   "total_traffic_month13",
                   "total_traffic_month14",
                   "total_traffic_month15",
                   "total_traffic_month16",
                   "total_traffic_month17")

        self.spark.sql("DROP TABLE IF EXISTS default.ds_modeller_ignite_tv_model")
        join_iptv_rhsi.write.saveAsTable("default.ds_modeller_ignite_tv_model")






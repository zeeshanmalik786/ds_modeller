from src.preprocessor.preprocess import preprocessor
from src.config.config import config
from pyspark.sql import functions as func
from pyspark.sql.functions import *


class ium:

    def __init__(self):

        self.con = config()
        self.obj = preprocessor(self.con.context)
        self.spark = self.con.spark



    def __ium_aggregations__(self, run_date):

        ium_df = self.obj.get_data("ium.mac_usage_fct", ["cm_mac_addr",
                                                         "cmts_host_name",
                                                         "cmts_md_if_index",
                                                         "state",
                                                         "event_date_utc"]).\
            filter((col("event_date_utc") >= date_sub(lit(run_date), 28)) & (col("event_date_utc") <= date_sub(lit(run_date),1)))

        # ----------------------------------------- One Day Aggregations ----------------------------------------------

        # iptv cable_modems

        ium_df_one_day_iptv = ium_df.filter(col("event_date_utc")==date_sub(lit(run_date),1)).\
            filter(col("state")=="2_WAVE3_MANAGED").\
            groupBy("cmts_host_name","cmts_md_if_index","state","event_date_utc").agg(countDistinct("cm_mac_addr")).\
            withColumnRenamed("count(cm_mac_addr)", "cable_modem_count_iptv").\
            select("cmts_host_name",
                   "cmts_md_if_index",
                   "state",
                   "cable_modem_count_iptv")

        # Ignite HSI cable modem

        ium_df_one_day_hsi = ium_df.filter(col("event_date_utc")==date_sub(lit(run_date),1)).\
                filter(col("state")=="1_WAVE3_HSI").\
                groupBy("cmts_host_name","cmts_md_if_index","state","event_date_utc").agg(countDistinct("cm_mac_addr")).\
            withColumnRenamed("count(cm_mac_addr)", "cable_modem_count_hsi").\
            select("cmts_host_name",
                   "cmts_md_if_index",
                   "state",
                   "cable_modem_count_hsi")

        # Legacy Modems

        ium_df_one_day_legacy = ium_df.filter(col("event_date_utc")==date_sub(lit(run_date),1)).\
                filter(col("state")=="Valid").\
                groupBy("cmts_host_name","cmts_md_if_index","state","event_date_utc").agg(countDistinct("cm_mac_addr")).\
            withColumnRenamed("count(cm_mac_addr)", "cable_modem_count_legacy")


        ium_agg_one_day = self.obj.join_three_frames(ium_df_one_day_iptv,ium_df_one_day_hsi,ium_df_one_day_legacy,
                                                                                                    "inner",
                                                                                                    ["cmts_host_name",
                                                                                                     "cmts_md_if_index"]).\
            select("cmts_host_name",
                   "cmts_md_if_index",
                   "cable_modem_count_iptv",
                   "cable_modem_count_hsi",
                   "cable_modem_count_legacy",
                   "event_date_utc")
        #---------------------------------------------------------------------------------------------------------------
        #-----------------------------------------4 weeks Average-------------------------------------------------------

        ium_df_4wk_iptv = ium_df.filter(col("state") == "2_WAVE3_MANAGED"). \
            groupBy("cmts_host_name", "cmts_md_if_index", "state", "event_date_utc").agg(countDistinct("cm_mac_addr")). \
            withColumnRenamed("count(cm_mac_addr)", "cable_modem_count_iptv").\
            groupBy("cmts_host_name", "cmts_md_if_index", "state").avg("cable_modem_count_iptv").\
            withColumnRenamed("avg(cable_modem_count_iptv)","4wk_avg_count_of_iptv")


        # Ignite HSI cable modem

        ium_df_4wk_hsi = ium_df.filter(col("state") == "1_WAVE3_HSI"). \
            groupBy("cmts_host_name", "cmts_md_if_index", "state", "event_date_utc").agg(countDistinct("cm_mac_addr")). \
            withColumnRenamed("count(cm_mac_addr)", "cable_modem_count_hsi"). \
            groupBy("cmts_host_name", "cmts_md_if_index", "state").avg("cable_modem_count_hsi"). \
            withColumnRenamed("avg(cable_modem_count_hsi)", "4wk_avg_count_of_hsi")


        # Legacy Modems
        ium_df_4wk_legacy = ium_df.filter(col("state") == "Valid"). \
            groupBy("cmts_host_name", "cmts_md_if_index", "state", "event_date_utc").agg(countDistinct("cm_mac_addr")). \
            withColumnRenamed("count(cm_mac_addr)", "cable_modem_count_legacy"). \
            groupBy("cmts_host_name", "cmts_md_if_index", "state").avg("cable_modem_count_legacy"). \
            withColumnRenamed("avg(cable_modem_count_legacy)", "4wk_avg_count_of_legacy")

        ium_agg_4wk = self.obj.join_three_frames(ium_df_4wk_iptv,
                                                 ium_df_4wk_hsi,
                                                 ium_df_4wk_legacy, "inner", ["cmts_host_name",
                                                                              "cmts_md_if_index"])

        ium_agg_one_day_and_4wk = self.obj.join_two_frames(ium_agg_one_day, ium_agg_4wk,"inner",["cmts_host_name",
                                                                                                 "cmts_md_if_index"]).\
            select("cmts_host_name",
                   "cmts_md_if_index",
                   "cable_modem_count_iptv",
                   "cable_modem_count_hsi",
                   "cable_modem_count_legacy",
                   "4wk_avg_count_of_iptv",
                   "4wk_avg_count_of_hsi",
                   "4wk_avg_count_of_legacy",
                   "event_date_utc").\
            withColumn("cmts_md_if_index",col("cmts_md_if_index").substr(-2,2))

        ium_agg_one_day_and_4wk.show()

        sam_report_topology = self.obj.get_data("cr_tim.sam34_cbu_view_cong",["PHUB",
                                                                       "CMTS_NAME",
                                                                       "MAC_DOMAIN"]).\
            withColumnRenamed("CMTS_NAME","cmts_host_name").\
            withColumn("cmts_md_if_index",col("MAC_DOMAIN").substr(-2,2)).distinct()

        sam_report_topology.show()

        self.obj.join_two_frames(sam_report_topology, ium_agg_one_day_and_4wk, "inner",["cmts_host_name",
                                                                                        "cmts_md_if_index"]).\
            select("PHUB",
                   "cmts_host_name",
                   "MAC_DOMAIN",
                   "cable_modem_count_iptv",
                   "cable_modem_count_hsi",
                   "cable_modem_count_legacy",
                   "4wk_avg_count_of_iptv",
                   "4wk_avg_count_of_hsi",
                   "4wk_avg_count_of_legacy",
                   "event_date_utc").\
            distinct().\
            withColumnRenamed("MAC_DOMAIN","cmts_md_if_index").write.saveAsTable("default.ds_modeler_ium_aggregations")






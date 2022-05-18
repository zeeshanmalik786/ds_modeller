from HFC.config import config
from HFC.preprocess import preprocessor
from pyspark.sql.functions import col, array_contains, element_at
from pyspark.sql import functions as func


class HFC_DOWNSTREAM:

    def __init__(self):
        self.con = config()
        self.obj = preprocessor(self.con.context)
        self.spark = self.con.spark
        self.utilization_impact_of_t3_t4 = 0.50
        self.congestion_threshold = 0.75
        self.adjustment_factor = 1


    def __hfc_downstream_approximations__(self):

        self.spark.sql("DROP TABLE IF EXISTS default.ds_modeller_hfc_modernization")

        self.obj.get_data("default.ds_modeller_ignite_tv_model", ["xref",
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
                                                                  "iptv_throughput",
                                                                  "md_perc_of_site",
                                                                  "md_iptv_sub_impact_mbps",
                                                                  "iptv_perc_of_total_cap",
                                                                  "current_md_rhsi_only_mbps",
                                                                  "current_perc_util_per_ium",
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
                                                                  "total_traffic_month17"]). \
            withColumn("utilm0",
                       (col("total_traffic_month0") / col("total_ds_capacity_in_mbps_scqam_ofdm")) * self.adjustment_factor).\
            withColumn("T1_1", func.when((col("utilm0") > self.congestion_threshold) & (col("current_ct_fwds_in_md") > 1),
                                         1).otherwise(0)). \
            withColumn("T34_1", func.when(
            (col("utilm0") > self.congestion_threshold) & (col("current_ct_fwds_in_md") == 1)  & (
                        col("T1_1") == 0), 1).otherwise(0)). \
            withColumn("adj_1", func.when((col("T1_1") == 1), self.adjustment_factor * (
                    (col("current_ct_fwds_in_md") - 1) / col("current_ct_fwds_in_md"))). \
                                 otherwise(
            func.when((col("T34_1") == 1), self.adjustment_factor * self.utilization_impact_of_t3_t4).otherwise(
                self.adjustment_factor))). \
            withColumn("utilm1",
                       (col("total_traffic_month1") / col("total_ds_capacity_in_mbps_scqam_ofdm")) * col("adj_1")). \
            withColumn("T1_2", func.when((col("utilm1") > self.congestion_threshold) & (col("current_ct_fwds_in_md") > 1),
                                         1).otherwise(0)). \
            withColumn("T34_2", func.when(
            (col("utilm1") > self.congestion_threshold) & (col("current_ct_fwds_in_md") == 1)  & (
                        col("T1_2") == 0), 1).otherwise(0)). \
            withColumn("adj_2", func.when((col("T1_2") == 1), col("adj_1") * (
                    (col("current_ct_fwds_in_md") - 1) / col("current_ct_fwds_in_md"))). \
                                 otherwise(
            func.when((col("T34_2") == 1), col("adj_1") * self.utilization_impact_of_t3_t4).otherwise(col("adj_1")))). \
            withColumn("utilm2",
                       (col("total_traffic_month2") / col("total_ds_capacity_in_mbps_scqam_ofdm")) * col("adj_2")).\
            withColumn("T1_3", func.when((col("utilm2") > self.congestion_threshold) & (col("current_ct_fwds_in_md") > 1),
                                         1).otherwise(0)). \
            withColumn("T34_3", func.when(
            (col("utilm2") > self.congestion_threshold) & (col("current_ct_fwds_in_md") == 1)  & (
                        col("T1_3") == 0), 1).otherwise(0)). \
            withColumn("adj_3", func.when((col("T1_3") == 1), col("adj_2") * (
                    (col("current_ct_fwds_in_md") - 1) / col("current_ct_fwds_in_md"))). \
                                 otherwise(
            func.when((col("T34_3") == 1), col("adj_2") * self.utilization_impact_of_t3_t4).otherwise(col("adj_2")))). \
            withColumn("utilm3",
                       (col("total_traffic_month3") / col("total_ds_capacity_in_mbps_scqam_ofdm")) * col("adj_3")). \
            withColumn("T1_4", func.when((col("utilm3") > self.congestion_threshold) & (col("current_ct_fwds_in_md") > 1),
                                         1).otherwise(0)). \
            withColumn("T34_4", func.when(
            (col("utilm3") > self.congestion_threshold) & (col("current_ct_fwds_in_md") == 1)  & (
                        col("T1_4") == 0), 1).otherwise(0)). \
            withColumn("adj_4", func.when((col("T1_4") == 1), col("adj_3") * (
                    (col("current_ct_fwds_in_md") - 1) / col("current_ct_fwds_in_md"))). \
                                 otherwise(
            func.when((col("T34_4") == 1), col("adj_3") * self.utilization_impact_of_t3_t4).otherwise(col("adj_3")))). \
            withColumn("utilm4",
                       (col("total_traffic_month4") / col("total_ds_capacity_in_mbps_scqam_ofdm")) * col("adj_4")). \
            withColumn("T1_5", func.when((col("utilm4") > self.congestion_threshold) & (col("current_ct_fwds_in_md") > 1),
                                         1).otherwise(0)). \
            withColumn("T34_5", func.when(
            (col("utilm4") > self.congestion_threshold) & (col("current_ct_fwds_in_md") == 1)  & (
                        col("T1_5") == 0), 1).otherwise(0)). \
            withColumn("adj_5", func.when((col("T1_5") == 1), col("adj_4") * (
                    (col("current_ct_fwds_in_md") - 1) / col("current_ct_fwds_in_md"))). \
                                 otherwise(
            func.when((col("T34_5") == 1), col("adj_4") * self.utilization_impact_of_t3_t4).otherwise(col("adj_4")))). \
            withColumn("utilm5",
                       (col("total_traffic_month5") / col("total_ds_capacity_in_mbps_scqam_ofdm")) * col("adj_5")). \
            withColumn("T1_6", func.when((col("utilm5") > self.congestion_threshold) & (col("current_ct_fwds_in_md") > 1),
                                         1).otherwise(0)). \
            withColumn("T34_6", func.when(
            (col("utilm5") > self.congestion_threshold) & (col("current_ct_fwds_in_md") == 1)  & (
                        col("T1_6") == 0), 1).otherwise(0)). \
            withColumn("adj_6", func.when((col("T1_6") == 1), col("adj_5") * (
                    (col("current_ct_fwds_in_md") - 1) / col("current_ct_fwds_in_md"))). \
                                 otherwise(
            func.when((col("T34_6") == 1), col("adj_5") * self.utilization_impact_of_t3_t4).otherwise(col("adj_5")))). \
            withColumn("utilm6",
                       (col("total_traffic_month6") / col("total_ds_capacity_in_mbps_scqam_ofdm")) * col("adj_6")). \
            withColumn("T1_7", func.when((col("utilm6") > self.congestion_threshold) & (col("current_ct_fwds_in_md") > 1),
                                         1).otherwise(0)). \
            withColumn("T34_7", func.when(
            (col("utilm6") > self.congestion_threshold) & (col("current_ct_fwds_in_md") == 1)  & (
                        col("T1_7") == 0), 1).otherwise(0)). \
            withColumn("adj_7", func.when((col("T1_7") == 1), col("adj_6") * (
                    (col("current_ct_fwds_in_md") - 1) / col("current_ct_fwds_in_md"))). \
                                 otherwise(
            func.when((col("T34_7") == 1), col("adj_6") * self.utilization_impact_of_t3_t4).otherwise(col("adj_6")))). \
            withColumn("utilm7",
                       (col("total_traffic_month7") / col("total_ds_capacity_in_mbps_scqam_ofdm")) * col("adj_7")). \
            withColumn("T1_8", func.when((col("utilm7") > self.congestion_threshold) & (col("current_ct_fwds_in_md") > 1),
                                         1).otherwise(0)). \
            withColumn("T34_8", func.when(
            (col("utilm7") > self.congestion_threshold) & (col("current_ct_fwds_in_md") == 1)  & (
                        col("T1_8") == 0), 1).otherwise(0)). \
            withColumn("adj_8", func.when((col("T1_8") == 1), col("adj_7") * (
                    (col("current_ct_fwds_in_md") - 1) / col("current_ct_fwds_in_md"))). \
                                 otherwise(
            func.when((col("T34_8") == 1), col("adj_7") * self.utilization_impact_of_t3_t4).otherwise(col("adj_7")))). \
            withColumn("utilm8",
                       (col("total_traffic_month8") / col("total_ds_capacity_in_mbps_scqam_ofdm")) * col("adj_8")). \
            withColumn("T1_9", func.when((col("utilm8") > self.congestion_threshold) & (col("current_ct_fwds_in_md") > 1),
                                         1).otherwise(0)). \
            withColumn("T34_9", func.when(
            (col("utilm8") > self.congestion_threshold) & (col("current_ct_fwds_in_md") == 1)  & (
                        col("T1_9") == 0), 1).otherwise(0)). \
            withColumn("adj_9", func.when((col("T1_9") == 1), col("adj_8") * (
                    (col("current_ct_fwds_in_md") - 1) / col("current_ct_fwds_in_md"))). \
                                 otherwise(
            func.when((col("T34_9") == 1), col("adj_8") * self.utilization_impact_of_t3_t4).otherwise(col("adj_8")))). \
            withColumn("utilm9",
                       (col("total_traffic_month9") / col("total_ds_capacity_in_mbps_scqam_ofdm")) * col("adj_9")). \
            withColumn("T1_10", func.when((col("utilm9") > self.congestion_threshold) & (col("current_ct_fwds_in_md") > 1),
                                          1).otherwise(0)). \
            withColumn("T34_10", func.when(
            (col("utilm9") > self.congestion_threshold) & (col("current_ct_fwds_in_md") == 1)  & (
                        col("T1_10") == 0), 1).otherwise(0)). \
            withColumn("adj_10", func.when((col("T1_10") == 1), col("adj_9") * (
                    (col("current_ct_fwds_in_md") - 1) / col("current_ct_fwds_in_md"))). \
                                 otherwise(
            func.when((col("T34_10") == 1), col("adj_9") * self.utilization_impact_of_t3_t4).otherwise(col("adj_9")))). \
            withColumn("utilm10",
                       (col("total_traffic_month10") / col("total_ds_capacity_in_mbps_scqam_ofdm")) * col("adj_10")). \
            withColumn("T1_11", func.when((col("utilm10") > self.congestion_threshold) & (col("current_ct_fwds_in_md") > 1),
                                          1).otherwise(0)). \
            withColumn("T34_11", func.when(
            (col("utilm10") > self.congestion_threshold) & (col("current_ct_fwds_in_md") == 1)  & (
                        col("T1_11") == 0), 1).otherwise(0)). \
            withColumn("adj_11", func.when((col("T1_11") == 1), col("adj_10") * (
                    (col("current_ct_fwds_in_md") - 1) / col("current_ct_fwds_in_md"))). \
                                 otherwise(
            func.when((col("T34_11") == 1), col("adj_10") * self.utilization_impact_of_t3_t4).otherwise(col("adj_10")))). \
            withColumn("utilm11",
                       (col("total_traffic_month11") / col("total_ds_capacity_in_mbps_scqam_ofdm")) * col("adj_11")). \
            withColumn("T1_12", func.when((col("utilm11") > self.congestion_threshold) & (col("current_ct_fwds_in_md") > 1),
                                          1).otherwise(0)). \
            withColumn("T34_12", func.when(
            (col("utilm11") > self.congestion_threshold) & (col("current_ct_fwds_in_md") == 1)  & (
                        col("T1_12") == 0), 1).otherwise(0)). \
            withColumn("adj_12", func.when((col("T1_12") == 1), col("adj_11") * (
                    (col("current_ct_fwds_in_md") - 1) / col("current_ct_fwds_in_md"))). \
                                 otherwise(
            func.when((col("T34_12") == 1), col("adj_11") * self.utilization_impact_of_t3_t4).otherwise(col("adj_11")))). \
            withColumn("utilm12",
                       (col("total_traffic_month12") / col("total_ds_capacity_in_mbps_scqam_ofdm")) * col("adj_12")). \
            withColumn("T1_13", func.when((col("utilm12") > self.congestion_threshold) & (col("current_ct_fwds_in_md") > 1),
                                          1).otherwise(0)). \
            withColumn("T34_13", func.when(
            (col("utilm12") > self.congestion_threshold) & (col("current_ct_fwds_in_md") == 1)  & (
                        col("T1_13") == 0), 1).otherwise(0)). \
            withColumn("adj_13", func.when((col("T1_13") == 1), col("adj_12") * (
                    (col("current_ct_fwds_in_md") - 1) / col("current_ct_fwds_in_md"))). \
                                 otherwise(
            func.when((col("T34_13") == 1), col("adj_12") * self.utilization_impact_of_t3_t4).otherwise(col("adj_12")))). \
            withColumn("utilm13",
                       (col("total_traffic_month13") / col("total_ds_capacity_in_mbps_scqam_ofdm")) * col("adj_13")). \
            withColumn("T1_14", func.when((col("utilm13") > self.congestion_threshold) & (col("current_ct_fwds_in_md") > 1),
                                          1).otherwise(0)). \
            withColumn("T34_14", func.when(
            (col("utilm13") > self.congestion_threshold) & (col("current_ct_fwds_in_md") == 1)  & (
                        col("T1_14") == 0), 1).otherwise(0)). \
            withColumn("adj_14", func.when((col("T1_14") == 1), col("adj_13") * (
                    (col("current_ct_fwds_in_md") - 1) / col("current_ct_fwds_in_md"))). \
                                 otherwise(
            func.when((col("T34_14") == 1), col("adj_13") * self.utilization_impact_of_t3_t4).otherwise(col("adj_13")))). \
            withColumn("utilm14",
                       (col("total_traffic_month14") / col("total_ds_capacity_in_mbps_scqam_ofdm")) * col("adj_14")). \
            withColumn("T1_15", func.when((col("utilm14") > self.congestion_threshold) & (col("current_ct_fwds_in_md") > 1),
                                          1).otherwise(0)). \
            withColumn("T34_15", func.when(
            (col("utilm14") > self.congestion_threshold) & (col("current_ct_fwds_in_md") == 1)  & (
                        col("T1_15") == 0), 1).otherwise(0)). \
            withColumn("adj_15", func.when((col("T1_15") == 1), col("adj_14") * (
                    (col("current_ct_fwds_in_md") - 1) / col("current_ct_fwds_in_md"))). \
                                 otherwise(
            func.when((col("T34_15") == 1), col("adj_14") * self.utilization_impact_of_t3_t4).otherwise(col("adj_14")))). \
            withColumn("utilm15",
                       (col("total_traffic_month15") / col("total_ds_capacity_in_mbps_scqam_ofdm")) * col("adj_15")). \
            withColumn("T1_16", func.when((col("utilm15") > self.congestion_threshold) & (col("current_ct_fwds_in_md") > 1),
                                          1).otherwise(0)). \
            withColumn("T34_16", func.when(
            (col("utilm15") > self.congestion_threshold) & (col("current_ct_fwds_in_md") == 1)  & (
                        col("T1_16") == 0), 1).otherwise(0)). \
            withColumn("adj_16", func.when((col("T1_16") == 1), col("adj_15") * (
                    (col("current_ct_fwds_in_md") - 1) / col("current_ct_fwds_in_md"))). \
                                 otherwise(
            func.when((col("T34_16") == 1), col("adj_15") * self.utilization_impact_of_t3_t4).otherwise(col("adj_15")))). \
            withColumn("utilm16",
                       (col("total_traffic_month16") / col("total_ds_capacity_in_mbps_scqam_ofdm")) * col("adj_16")). \
            withColumn("T1_17", func.when((col("utilm16") > self.congestion_threshold) & (col("current_ct_fwds_in_md") > 1),
                                          1).otherwise(0)). \
            withColumn("T34_17", func.when(
            (col("utilm16") > self.congestion_threshold) & (col("current_ct_fwds_in_md") == 1)  & (
                        col("T1_17") == 0), 1).otherwise(0)). \
            withColumn("adj_17", func.when((col("T1_17") == 1), col("adj_16") * (
                    (col("current_ct_fwds_in_md") - 1) / col("current_ct_fwds_in_md"))). \
                                 otherwise(
            func.when((col("T34_17") == 1), col("adj_16") * self.utilization_impact_of_t3_t4).otherwise(col("adj_16")))).\
            write.saveAsTable("default.ds_modeller_hfc_modernization")

        return True


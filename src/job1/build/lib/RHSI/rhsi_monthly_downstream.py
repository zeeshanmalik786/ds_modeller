from RHSI.config import config
from RHSI.preprocess import preprocessor
from pyspark.sql.functions import col, array_contains, element_at



class RHSI:

    def __init__(self):
        self.con = config()
        self.obj = preprocessor(self.con.context)
        self.spark = self.con.spark


    def __rhsi_calculation__(self):



        # RHSI ALL

        down_bytes = self.obj.get_data("default.ds_modeller_down_byte_daily", "*")

        # RHSI ALL Approximations

        rhsi_approx = down_bytes.withColumn("RHSI_Month_0", col("current_md_rhsi_only_mbps")). \
            withColumn("RHSI_Month_1", col("RHSI_Month_0") * col("regional_rhsi_cmgr")). \
            withColumn("RHSI_Month_2", col("RHSI_Month_1") * col("regional_rhsi_cmgr")). \
            withColumn("RHSI_Month_3", col("RHSI_Month_2") * col("regional_rhsi_cmgr")). \
            withColumn("RHSI_Month_4", col("RHSI_Month_3") * col("regional_rhsi_cmgr")). \
            withColumn("RHSI_Month_5", col("RHSI_Month_4") * col("regional_rhsi_cmgr")). \
            withColumn("RHSI_Month_6", col("RHSI_Month_5") * col("regional_rhsi_cmgr")). \
            withColumn("RHSI_Month_7", col("RHSI_Month_6") * col("regional_rhsi_cmgr")). \
            withColumn("RHSI_Month_8", col("RHSI_Month_7") * col("regional_rhsi_cmgr")). \
            withColumn("RHSI_Month_9", col("RHSI_Month_8") * col("regional_rhsi_cmgr")). \
            withColumn("RHSI_Month_10", col("RHSI_Month_9") * col("regional_rhsi_cmgr")). \
            withColumn("RHSI_Month_11", col("RHSI_Month_10") * col("regional_rhsi_cmgr")). \
            withColumn("RHSI_Month_12", col("RHSI_Month_11") * col("regional_rhsi_cmgr")). \
            withColumn("RHSI_Month_13", col("RHSI_Month_12") * col("regional_rhsi_cmgr")). \
            withColumn("RHSI_Month_14", col("RHSI_Month_13") * col("regional_rhsi_cmgr")). \
            withColumn("RHSI_Month_15", col("RHSI_Month_14") * col("regional_rhsi_cmgr")). \
            withColumn("RHSI_Month_16", col("RHSI_Month_15") * col("regional_rhsi_cmgr")). \
            withColumn("RHSI_Month_17", col("RHSI_Month_16") * col("regional_rhsi_cmgr"))

        self.spark.sql("DROP TABLE IF EXISTS default.ds_modeller_down_byte_daily_RHSI_Approximation")
        rhsi_approx.write.saveAsTable("default.ds_modeller_down_byte_daily_RHSI_Approximation")

        return True



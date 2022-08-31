from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, max


class ChargePointsETLJob:
    input_path = './electric-chargepoints-2017.csv'
    output_path = './chargepoints-2017-analysis'

    def __init__(self):
        self.spark_session = (SparkSession.builder
                              .master("local[*]")
                              .appName("ElectricChargePointsETLJob")
                              .getOrCreate())

    def extract(self):
        extract_df = self.spark_session.read.option('header', True).csv(self.input_path)
        return extract_df

    def transform(self, df):
        df.drop('ChargingEvent', 'StartDate', 'StartTime', 'EndDate', 'EndTime', 'Energy')

        transform_df = df.groupBy('CPID').agg(max("PluginDuration").alias("max_duration"),
                                              avg("PluginDuration").alias("avg_duration"))
        transform_df = transform_df.withColumnRenamed('CPID', 'Charging_Point')
        #transform_df.show()
        return transform_df

    def load(self, df):
        """will create 200 file because 200 is default number of partitions(jobs). this will run parallely """
        #return df.write.option('header', True).csv(self.output_path)
        """Below methods will create 1 file but won't run paralley"""
        #return df.repartition(1).write.option('header', True).csv(self.output_path)
        return #df.coalesce(1).write.option('header', True).csv(self.output_path)

    def run(self):
        self.load(self.transform(self.extract()))

c = ChargePointsETLJob()
c.run()

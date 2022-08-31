# ETL
ETL with Pyspark

Here we trying to perform simple ETL (Extract , Transform , Load) operation with Pyspark

We are using sample input CSV file .
electric-chargepoints-2017.csv inculde information of charging at EV station. 
+-------------+-------+----------+---------+----------+--------+------+------------------+
|ChargingEvent|   CPID| StartDate|StartTime|   EndDate| EndTime|Energy|    PluginDuration|
+-------------+-------+----------+---------+----------+--------+------+------------------+
|     16673806|AN11719|2017-12-31| 14:46:00|2017-12-31|18:00:00|   2.4|3.2333333333333334|
|     16670986|AN01706|2017-12-31| 11:25:00|2017-12-31|13:14:00|   6.1|1.8166666666666667|
|      3174961|AN18584|2017-12-31| 11:26:11|2018-01-01|12:54:11|    24|25.466666666666665|
|     16674334|AN00812|2017-12-31| 15:18:00|2018-01-01|14:06:00|   6.7|              22.8|
+-------------+-------+----------+---------+----------+--------+------+------------------+
Extract 
Extract csv into spark dataframe.

Transform - 

Getting average PluginDuration at each Charging station (CPID)
get Max PluginDuration at each Charging station (CPID)
rename CPID to Chargin_point


+--------------+------------------+------------------+
|Charging_Point|      max_duration|      avg_duration|
+--------------+------------------+------------------+
|       AN00218|3.4594444444444443| 1.556851851851852|
|       AN00603| 7.783333333333333| 28.71675925925926|
|       AN03946| 4.033333333333333| 7.879444444444444|
|       AN04630| 34.32388888888889|22.939351851851853|
+--------------+------------------+------------------+

Load -
Load Dataframe to output csv.

Note - 

Spark version 3.1.2
Hadoop version  - 3.2
Pyspark version - 3.1.2
Java - 8 +

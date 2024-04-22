from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark= SparkSession.builder.appName("Aggregate_by_pick_up_date").master("local").getOrCreate() 

df=spark.read.csv("/user/root/bookings_1/part-m-00000")
new_col = ["booking_id","customer_id","driver_id","customer_app_version","customer_phone_os_version","pickup_lat","pickup_lon","drop_lat",
          "drop_lon","pickup_timestamp","drop_timestamp","trip_fare","tip_amount","currency_code","cab_color","cab_registration_no","customer_rating_by_driver",
          "rating_by_customer","passenger_count"]

#Creating a new Dataframe form the old data frame and assigning a new schema 
new_df = df.toDF(*new_col)

new_df.show(5)

# Crearing a new column which has only the date part from the pickup_timestamp column 
df_new= new_df.withColumn("Pickupdate",col("pickup_timestamp").cast("date"))
df_new.show(5)


# grouping the data by the new column Pickupdate
agg_df = df_new.groupBy("Pickupdate").agg(count("booking_id").alias("booking_count")).orderBy("Pickupdate")


# Copying the data to a csv file 
agg_df.coalesce(1).write.format('csv').mode('overwrite').save('/user/root/datewise_bookings_agg',header='true')



package SetTopBoxes.Trash

import org.apache.spark.sql.{SparkSession, _}

/**
  * Created by yurav on 19/04/2017.
  */
object Devices_Watch_History_By_Devices {
  val spark:SparkSession = SparkSession
    .builder()
    .master("local")
    .appName("Scala Spark SQL")
    .config("spark.sql.warehouse.dir", "file:///tmp/spark-warehouse")
    .config("spark.speculation","false")
    .getOrCreate();

  /** Main function */
  def main(args: Array[String]): Unit = {

    import spark.implicits._

    val views:DataFrame = spark.read
      .format("com.databricks.spark.csv")
      .option("delimiter", "|")
      //.load("s3n://magnet-fwm/home/TsviKuflik/devices_views/devices_120_hist/*")
      .load("D:\\SintecMediaData\\devices_views\\devices_120_hist\\*.gz")
      .withColumnRenamed("_c0","device_id_unique")
      .withColumnRenamed("_c1","log_type")
      .withColumnRenamed("_c2","event_date")
      .withColumnRenamed("_c3","event_time")
      .withColumnRenamed("_c4","station_num")
      .withColumnRenamed("_c5","prog_code")
      .withColumnRenamed("_c6","event_type")
      .withColumnRenamed("_c7","event_value")
      .withColumnRenamed("_c8","event_name")
      .withColumnRenamed("_c9","event_id")
//      .repartition($"device_id_unique")
//      .mapPartitions(
//        r=>r.toList
//          .sortBy(e=>e.getString(0) + e.getString(1)).toIterator
//      )
      .sort($"event_date",$"event_time")
      //.withColumn("date_time", concat( regexp_replace($"event_date","2015",""),$"event_time").cast("Int"))
      //.orderBy($"date_time")

    views
      .write
      .partitionBy("device_id_unique")
      .mode(SaveMode.Append)
      //.sortBy("date_time")
      .format("com.databricks.spark.csv")
      .option("codec", "org.apache.hadoop.io.compress.GzipCodec")
      .option("header",true)
      //.save("s3://magnet-fwm/home/TsviKuflik/devices_views/devices_120_hist_by_device")
      .save("D:\\SintecMediaData\\devices_views\\devices_120_hist_by_device")

    //views.show()
    print("done!")

  }
}
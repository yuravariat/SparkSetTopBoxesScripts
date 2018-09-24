package SetTopBoxes.Devices

import java.text.SimpleDateFormat

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SparkSession, _}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
  * Created by yurav on 19/04/2017.
  */
object Create_Devices_Appearances_By_Views {
  @transient val conf: SparkConf = new SparkConf().setMaster("local").setAppName("Devices")
  @transient val sc: SparkContext = new SparkContext(conf)

  /** Main function */
  def main(args: Array[String]): Unit = {

    sc.getConf.set("fs.s3n.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
    sc.getConf.set("fs.s3n.awsAccessKeyId", "")
    sc.getConf.set("fs.s3n.awsSecretAccessKey", "")

    val spark: SparkSession = SparkSession
      .builder()
      .master("local")
      .appName("Scala Spark SQL")
      .config("spark.sql.warehouse.dir", "file:///tmp/spark-warehouse")
      .getOrCreate()

    import spark.implicits._

    //val format = new SimpleDateFormat("yyyy-MM-dd\\THH:mm:ss.fff\\Z")
    val simpleFormat = new SimpleDateFormat("yyyy-MM-dd")

    def DatePart = udf((date: java.sql.Timestamp) => {
      if (date == null) {
        ""
      }
      else {
        simpleFormat.format(date)
      }
    })

    def ReduceDates = udf((dates: mutable.WrappedArray[String]) => {
      if (dates == null) {
        null
      }
      else {
        dates.array.filter(d => d != null)
          .map(d => (d, 1))
          .groupBy(t => t._1)
          .map(g => (g._1, g._2
            .reduce((t1, t2) => (t1._1, t1._2 + t2._2))._2))
          .toArray.sortBy(t => t._1).map(t => t._1 + ":" + t._2).mkString("|")
      }
    })

    val ViewsDF = spark.read
      .format("com.databricks.spark.csv")
      .load("s3n://magnet-fwm/home/TsviKuflik/y_views_with_durations/part*.gz")
      .withColumnRenamed("_c0", "deviceID")
      .withColumnRenamed("_c1", "event_date_time")
      //.withColumnRenamed("_c2", "station_num")
      .withColumnRenamed("_c3", "prog_code")
      .withColumnRenamed("_c4", "duration")
      //.withColumnRenamed("_c5", "prog_duration")
      .where($"prog_code" =!= "MP_MISSINGPROGRAM" && $"prog_code" =!= "DVRPGMKEY" && $"duration" > 5)
      .withColumn("date", DatePart($"event_date_time"))
      .groupBy($"deviceID")
      .agg(
        ReduceDates(collect_list($"date")) as "dates")

    //ViewsDF.printSchema()
    //ViewsDF.show(1)

    ViewsDF.write
      .format("com.databricks.spark.csv")
      .option("codec", "org.apache.hadoop.io.compress.GzipCodec")
      .save("s3n://magnet-fwm/home/TsviKuflik/y_devices/devices_appearances_by_views")

  }
}

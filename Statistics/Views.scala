package SetTopBoxes.Statistics

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by yurav on 19/04/2017.
  */
object Views {
  @transient val conf: SparkConf = new SparkConf().setMaster("local").setAppName("Devices")
  @transient val sc: SparkContext = new SparkContext(conf)

  val spark: SparkSession = SparkSession
    .builder()
    .master("local")
    .appName("Scala Spark SQL")
    .config("spark.sql.warehouse.dir", "file:///tmp/spark-warehouse")
    .getOrCreate()

  import spark.implicits._

  /** Main function */
  def main(args: Array[String]): Unit = {

    sc.getConf.set("fs.s3n.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
    sc.getConf.set("fs.s3n.awsAccessKeyId", "")
    sc.getConf.set("fs.s3n.awsSecretAccessKey", "")

    //<editor-fold desc="Run area">

    import spark.implicits._
    import org.apache.spark.sql.functions._

    println("\n" + "#" * 70 + " Statistics views " + "#" * 70 + "\n")

    val viewsDF = spark.read
      .format("com.databricks.spark.csv")
      .load("s3n://magnet-fwm/home/TsviKuflik/y_views_with_durations/*.gz")
      .select(
        $"_c0".as("deviceIdUniqe"),
        $"_c1".as("time"),
        $"_c2".as("channel"),
        $"_c3".as("program"),
        $"_c4".as("duration"),
        $"_c5".as("prog_duration"))

    //println("views count:" + viewsDF.count())
    //println("views count more than 5 minutes: " + viewsDF.where($"duration" > 5).count())

    sc.textFile("s3n://magnet-fwm/home/TsviKuflik/y_devices/devices_appearances_by_views/*.gz")
      .map(x => {
        val firstComma = x.indexOf(",")
        val device_uniq_id = x.substring(0, firstComma)
        var count = 0
        if (firstComma > 0 && x.length > (firstComma + 3)) {
          count = x.substring(firstComma + 1, x.length).split("\\|").length
          if (count < 365) {
            if (count > 300) {
              count = 300
            }
            else if (count > 250) {
              count = 250
            }
            else if (count > 200) {
              count = 200
            }
            else if (count > 150) {
              count = 150
            }
            else if (count > 100) {
              count = 100
            }
            else if (count > 50) {
              count = 50
            }
            else{
              count=1
            }
          }
        }
        (count, device_uniq_id)
      })
      .toDF("days", "device_id")
      .groupBy($"days")
      .agg(count($"device_id").as("devices_num"))
      .orderBy(-$"days")
      .show()

    println("done!")

    //</editor-fold>
  }
}

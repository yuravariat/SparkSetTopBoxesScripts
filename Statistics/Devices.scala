package SetTopBoxes.Statistics

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by yurav on 19/04/2017.
  */
object Devices {
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

    println("\n" + "#" * 70 + " Statistics devices " + "#" * 70 + "\n")

    val devicesDF = spark.read
      .format("com.databricks.spark.csv")
      .load("s3n://magnet-fwm/home/TsviKuflik/y_devices/unique_devices/*.gz")
      .select(
        $"_c0".as("deviceIdUniqe"),
        $"_c1".as("msoCode"),
        $"_c2".as("mso"),
        $"_c3".as("deviceID"),
        $"_c4".as("houseHoldID"),
        $"_c5".as("householdType"),
        $"_c6".as("zipcode"),
        $"_c7".as("city"),
        $"_c8".as("cityCode"),
        $"_c9".as("systemType"))

    devicesDF
      .groupBy($"houseHoldID")
      .agg(count($"deviceIdUniqe").as("devices_num"))
      .groupBy($"devices_num")
      .agg(count($"houseHoldID").as("house_holds_num"))
      .orderBy($"devices_num")
    //.show(200)

    devicesDF
      .groupBy($"city")
      .agg(
        countDistinct($"houseHoldID").as("households_num"),
        count($"deviceIdUniqe").as("devices_num"))
      .orderBy(-$"households_num")
      //.printSchema()
      .rdd
      .map(r => r.getString(0) + "|" + r.getLong(1) + "|" + r.getLong(2))
      .collect()
      .foreach(r => println(r))

    println("done!")

    //</editor-fold>
  }
}

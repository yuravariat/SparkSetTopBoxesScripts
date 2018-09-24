package SetTopBoxes.Devices

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SparkSession, _}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by yurav on 19/04/2017.
  */
object Create_Unique_Devices_List {
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

    //<editor-fold desc="Run area">

    val groupedByDeviceAndHouseDF = spark.read
      .format("com.databricks.spark.csv")
      .load("s3n://magnet-fwm/home/TsviKuflik/refcsv/SintecMedia.rpt_refcsv.date_*.csv.gz")
      .select(
        $"_c0".as("date"),
        $"_c1".as("msoCode"),
        $"_c2".as("mso"),
        $"_c3".as("deviceID"),
        $"_c4".as("houseHoldID"),
        $"_c5".as("householdType"),
        $"_c6",
        $"_c7",
        $"_c8",
        $"_c9".as("systemType"))
      .withColumn("deviceIdUniqe", concat_ws("-", $"msoCode", $"deviceID"))
      .withColumn("zipcode", when($"_c6" === "", null).otherwise($"_c6"))
      .withColumn("city", when($"_c7" === "Unknown" || $"_c7" === "", null).otherwise($"_c7"))
      .withColumn("cityCode", when($"_c8" === "", null).otherwise($"_c8"))
      .groupBy($"deviceIdUniqe")
      .agg(
        first($"msoCode").as("msoCode"),
        first($"mso").as("mso"),
        first($"deviceID").as("deviceID"),
        first($"houseHoldID").as("houseHoldID"),
        first($"householdType").as("householdType"),
        first($"zipcode", ignoreNulls = true).as("zipcode"),
        first($"city", ignoreNulls = true).as("city"),
        //first($"city", ignoreNulls = true).as("cityname"), // twice because of prtition by city.
        first($"cityCode", ignoreNulls = true).as("cityCode"),
        first($"systemType", ignoreNulls = true).as("systemType"))

    //groupedByDeviceAndHouseDF.show()

    println("saving groupedByDeviceAndHouseDF")
    groupedByDeviceAndHouseDF.write
      //.partitionBy("city")
      .format("com.databricks.spark.csv")
      .option("codec", "org.apache.hadoop.io.compress.GzipCodec")
      .save("s3n://magnet-fwm/home/TsviKuflik/y_devices/unique_devices")

    var houseHoldsWithOneDevice = groupedByDeviceAndHouseDF
      .groupBy($"houseHoldID")
      .agg(
        first($"deviceIdUniqe").as("deviceIdUniqe"),
        first($"msoCode").as("msoCode"),
        first($"mso").as("mso"),
        first($"deviceID").as("deviceID"),
        first($"householdType").as("householdType"),
        first($"zipcode", ignoreNulls = true).as("zipcode"),
        first($"city", ignoreNulls = true).as("city"),
        //first($"city", ignoreNulls = true).as("cityname"), // twice because of prtition by city.
        first($"cityCode", ignoreNulls = true).as("cityCode"),
        first($"systemType", ignoreNulls = true).as("systemType"),
        count($"deviceID").as("devicesCount"))
      .where($"devicesCount" === 1)

    println("saving houseHoldsWithOneDevice")
    houseHoldsWithOneDevice.write
      //.partitionBy("city")
      .format("com.databricks.spark.csv")
      .option("codec", "org.apache.hadoop.io.compress.GzipCodec")
      .save("s3n://magnet-fwm/home/TsviKuflik/y_devices/unique_devices_one_dev_in_house")


    // Check
    val devices = sc.textFile("s3n://magnet-fwm/home/TsviKuflik/y_devices/unique_devices/*.gz")
    println("total devices " + devices.count())

    val devicesHoldsWithOneDevice = sc.textFile("s3n://magnet-fwm/home/TsviKuflik/y_devices/unique_devices_one_dev_in_house/*.gz")
    println("total devices with one device in household " + devicesHoldsWithOneDevice.count())

    println("done!")

    //</editor-fold>

  }
}

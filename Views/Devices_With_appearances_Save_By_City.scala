package SetTopBoxes.Views

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SparkSession, _}

/**
  * Created by yurav on 19/04/2017.
  */
object Devices_With_appearances_Save_By_City {

  val spark: SparkSession = SparkSession
    .builder()
    .master("local")
    .appName("Scala Spark SQL")
    .config("spark.sql.warehouse.dir", "file:///tmp/spark-warehouse")
    .getOrCreate();

  import spark.implicits._

  /** Main function */
  def main(args: Array[String]): Unit = {

    val cities = List[String]("Abilene-Sweetwater", "Lake Charles", "Seattle-Tacoma")

    val devicesDF: DataFrame = spark.read
      .format("com.databricks.spark.csv")
      .load("s3n://magnet-fwm/home/TsviKuflik/devices/devices_appearances/*.gz")
      .withColumnRenamed("_c0", "deviceIUniqueD")
      .withColumnRenamed("_c1", "msoCode")
      .withColumnRenamed("_c2", "mso")
      .withColumnRenamed("_c3", "deviceID")
      .withColumnRenamed("_c4", "houseHoldID")
      .withColumnRenamed("_c5", "householdType")
      .withColumnRenamed("_c6", "zipcode")
      .withColumnRenamed("_c7", "city")
      .withColumnRenamed("_c8", "cityCode")
      .withColumnRenamed("_c9", "systemType")
      .withColumnRenamed("_c10", "days")
      .withColumnRenamed("_c11", "dates")

    val test = cities.contains("")

    //devices.show()

    var filteredDevices = devicesDF
      .where($"city" isin (cities: _*))

    //filteredDevices.count()

    var houseHoldsWithOneDevice = filteredDevices
      .groupBy($"houseHoldID".as("holdID"))
      .agg(count($"deviceID").as("devicesCount"))
      .where($"devicesCount" === 1)

    //houseHoldsWithOneDevice.count()

    var finalDF = filteredDevices
      .join(houseHoldsWithOneDevice,
        filteredDevices.col("houseHoldID") === houseHoldsWithOneDevice.col("holdID"))
      .where($"days" > 120)
      .where($"dates".startsWith("2015-01-01"))
      .cache()

    //finalDF.count()

    cities.foreach(city => {

      val dfToSave = finalDF.where($"city" === city)

      println(city + " " + dfToSave.count() + " rows")

      dfToSave
        .coalesce(1)
        .write
        .format("com.databricks.spark.csv")
        .option("codec", "org.apache.hadoop.io.compress.GzipCodec")
        .option("header", value = true)
        .save("s3://magnet-fwm/home/TsviKuflik/devices/devices_120_hist_" + city)
    })

    print("done!")

  }
}
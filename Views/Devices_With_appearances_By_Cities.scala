package SetTopBoxes.Views

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SparkSession, _}

/**
  * Created by yurav on 19/04/2017.
  */
object Devices_With_appearances_By_Cities {

  val spark:SparkSession = SparkSession
    .builder()
    .master("local")
    .appName("Scala Spark SQL")
    .config("spark.sql.warehouse.dir", "file:///tmp/spark-warehouse")
    .getOrCreate();

  import spark.implicits._

  /** Main function */
  def main(args: Array[String]): Unit = {

    val devices:DataFrame = spark.read
      .format("com.databricks.spark.csv")
      .load("s3n://magnet-fwm/home/TsviKuflik/devices/devices_appearances/*.gz")
      .withColumnRenamed("_c0","deviceIUniqueD")
      .withColumnRenamed("_c1","msoCode")
      .withColumnRenamed("_c2","mso")
      .withColumnRenamed("_c3","deviceID")
      .withColumnRenamed("_c4","houseHoldID")
      .withColumnRenamed("_c5","householdType")
      .withColumnRenamed("_c6","zipcode")
      .withColumnRenamed("_c7","city")
      .withColumnRenamed("_c8","cityCode")
      .withColumnRenamed("_c9","systemType")
      .withColumnRenamed("_c10","days")
      .withColumnRenamed("_c11","dates")

    var devicesByCities = devices
      .where($"days"===365) // with history for full year
      .groupBy($"city")
      .agg(count($"deviceID").as("devices"))
      .orderBy(-$"devices")

    devicesByCities.show()

    print("done!")

  }
}

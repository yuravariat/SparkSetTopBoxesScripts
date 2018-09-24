package SetTopBoxes.Views

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SparkSession, _}

/**
  * Created by yurav on 19/04/2017.
  */
object Devices_With_appearances_Show_Days_Counts {

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
      .withColumnRenamed("_c0","deviceID")
      .withColumnRenamed("_c1","msoCode")
      .withColumnRenamed("_c2","mso")
      .withColumnRenamed("_c3","houseHoldID")
      .withColumnRenamed("_c4","householdType")
      .withColumnRenamed("_c5","zipcode")
      .withColumnRenamed("_c6","city")
      .withColumnRenamed("_c7","cityCode")
      .withColumnRenamed("_c8","systemType")
      .withColumnRenamed("_c9","days")
      .withColumnRenamed("_c10","dates")

    var countsDF = devices
      .groupBy($"days")
      .agg(count($"deviceID").as("devices"))
      .orderBy(-$"days".cast("Int"))

    countsDF.show()

    print("done!")

  }
}

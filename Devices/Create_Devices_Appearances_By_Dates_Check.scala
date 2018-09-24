package SetTopBoxes.Devices

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SparkSession, _}

/**
  * Created by yurav on 19/04/2017.
  */
object Create_Devices_Appearances_By_Dates_Check {

  val spark:SparkSession = SparkSession
    .builder()
    .master("local")
    .appName("Scala Spark SQL")
    .config("spark.sql.warehouse.dir", "file:///tmp/spark-warehouse")
    .getOrCreate();

  import spark.implicits._

  /** Main function */
  def main(args: Array[String]): Unit = {

    val HouseHoldsDF:DataFrame = spark.read
      .format("com.databricks.spark.csv")
      .load("D:\\SintecMediaData\\refcsv\\SintecMedia.rpt_refcsv.date_2015-01-02.2016-11-21.csv.gz")

    //HouseHoldsDF.show(20)

    val groupedByDevicesDF = HouseHoldsDF
      .select($"_c0".as("date"),
        $"_c1".as("msoCode"),
        $"_c2".as("mso"),
        $"_c3".as("deviceID"),
        $"_c4".as("houseHoldID"),
        $"_c5".as("householdType"),
        $"_c6",
        $"_c7",
        $"_c8",
        $"_c9".as("systemType"))
      .withColumn("deviceIdUniqe",concat_ws("-",$"msoCode", $"deviceID"))
      .groupBy($"deviceIdUniqe")
      .agg(
        count($"deviceID").as("days"),
        concat_ws("|",sort_array(collect_list($"date"))).as("dates"))
      .where($"days">1)

    groupedByDevicesDF.count()
    groupedByDevicesDF.show()

  }
}

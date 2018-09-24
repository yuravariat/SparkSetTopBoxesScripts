package SetTopBoxes.Devices

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SparkSession, _}

/**
  * Created by yurav on 19/04/2017.
  */
object Create_Devices_Appearances_By_Dates {
  @transient val conf: SparkConf = new SparkConf().setMaster("local").setAppName("Devices")
  @transient val sc: SparkContext = new SparkContext(conf)

  val spark:SparkSession = SparkSession
    .builder()
    .master("local")
    .appName("Scala Spark SQL")
    .config("spark.sql.warehouse.dir", "file:///tmp/spark-warehouse")
    .getOrCreate();

  import spark.implicits._

  /** Main function */
  def main(args: Array[String]): Unit = {

    sc.getConf.set("fs.s3n.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
    sc.getConf.set("fs.s3n.awsAccessKeyId", "")
    sc.getConf.set("fs.s3n.awsSecretAccessKey", "")

    val HouseHoldsDF:DataFrame = spark.read
      .format("com.databricks.spark.csv")
      .load("s3n://magnet-fwm/home/TsviKuflik/refcsv/SintecMedia.rpt_refcsv.date_*.csv.gz")

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
      .withColumn("zipcode",when($"_c6" ==="", null).otherwise($"_c6"))
      .withColumn("city",when($"_c7" === "Unknown" || $"_c7"==="", null).otherwise($"_c7"))
      .withColumn("cityCode",when($"_c8" ==="", null).otherwise($"_c8"))
      .groupBy($"deviceIdUniqe")
      .agg(
        first($"msoCode"),
        first($"mso"),
        first($"deviceID"),
        first($"houseHoldID"),
        first($"householdType"),
        first($"zipcode",ignoreNulls = true),
        first($"city",ignoreNulls = true),
        first($"cityCode",ignoreNulls = true),
        first($"systemType"),
        count($"deviceID").as("days"),
        concat_ws("|",sort_array(collect_list($"date"))).as("dates"))

    groupedByDevicesDF.show()

//    groupedByDevicesDF.write
//      .coalesce(25)
//      .format("com.databricks.spark.csv")
//      .option("codec", "org.apache.hadoop.io.compress.GzipCodec")
//      .save("D:\\SintecMediaData\\devices\\devices_appearances")

  }
}

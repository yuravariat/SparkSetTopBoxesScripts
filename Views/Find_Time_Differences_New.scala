package SetTopBoxes.Views

import java.sql.Timestamp
import java.util.concurrent.TimeUnit

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{SparkSession, _}

/**
  * Created by yurav on 19/04/2017.
  */
object Find_Time_Differences_New {

  val spark: SparkSession = SparkSession
    .builder()
    .master("local")
    .appName("Scala Spark SQL")
    .config("spark.sql.warehouse.dir", "file:///tmp/spark-warehouse")
    .getOrCreate()

  import spark.implicits._

  /** Main function */
  def main(args: Array[String]): Unit = {

    val cities = List[(String, Long)](
      ("Amarillo", TimeUnit.HOURS.toMillis(5)),
      ("Parkersburg", TimeUnit.HOURS.toMillis(4)),
      ("Little Rock-Pine Bluff", TimeUnit.HOURS.toMillis(5)),
      ("Seattle-Tacoma", TimeUnit.HOURS.toMillis(7)))

    val datesToCheck = List[String]("2015-04-02", "2015-04-03", "2015-04-04", "2015-04-04", "2015-04-14", "2015-04-16"
      , "2015-04-22", "2015-04-24", "2015-04-28")
    for (dateToCheck <- datesToCheck) {

      println("#" * 50 + " " + dateToCheck + " " + "#" * 50)

      val dateToCheckShort = dateToCheck.replace("-", "")

      val programsSource = "s3n://magnet-fwm/home/TsviKuflik/rpt_programs/SintecMedia.rpt_programs.date_" + dateToCheck + "*.pd.gz"
      val viewsSource = "s3n://magnet-fwm/home/TsviKuflik/rpt_prog_view/SintecMedia.rpt_prog_view.date_" + dateToCheck + "*.pd.gz"

      for (i <- cities.indices) {
        val cityname = cities(i)._1
        val devicesSource = "s3n://magnet-fwm/home/TsviKuflik/y_devices/unique_devices/*.gz"

        val devices: List[String] = spark.read
          .format("com.databricks.spark.csv")
          .load(devicesSource)
          .withColumnRenamed("_c0", "deviceIUniqueD")
          .where($"_c7" === cityname)
          .select($"deviceIUniqueD")
          .map(r => r.getString(0))
          .take(5000).toList

        def AddMillisec = udf((t: Timestamp) => {
          new java.sql.Timestamp(t.getTime + cities(i)._2)
        })

        val views: DataFrame = spark.read
          .format("com.databricks.spark.csv")
          .option("delimiter", "|")
          .load(viewsSource)
          .withColumnRenamed("_c0", "msoCode")
          .withColumnRenamed("_c1", "deviceID")
          .withColumnRenamed("_c2", "event_date")
          .withColumnRenamed("_c3", "event_time")
          //.withColumnRenamed("_c4", "station_num")
          .withColumnRenamed("_c5", "prog_code")
          .withColumn("deviceIdUniqe", concat_ws("-", $"msoCode", $"deviceID"))
          .withColumn("event_date_time", unix_timestamp(concat($"event_date", $"event_time"), "yyyyMMddHHmmss").cast("Timestamp"))
          .where($"deviceIdUniqe".isin(devices: _*) && $"event_date" === dateToCheckShort)
          .groupBy($"prog_code")
          .agg(
            first("deviceID").as("deviceID"),
            //first("event_date").as("event_date"),
            first("event_time").as("event_time"),
            first("event_date_time").as("event_date_time"),
            count($"deviceID").as("views"))
          .where($"views" === 1)

        //views.show()

        val programs: DataFrame = spark.read
          .format("com.databricks.spark.csv")
          .option("delimiter", "|")
          .load(programsSource)
          .withColumnRenamed("_c0", "prog_id")
          //.withColumnRenamed("_c1", "prog_name")
          //.withColumnRenamed("_c2", "genres")
          .withColumnRenamed("_c3", "start_date")
          .withColumnRenamed("_c4", "start_time")
          .withColumnRenamed("_c5", "duration")
          .withColumn("start_date_time", AddMillisec(unix_timestamp(concat($"start_date", $"start_time"), "yyyyMMddHHmmss").cast("Timestamp")))
          .where($"start_date" === dateToCheckShort)
          .groupBy($"prog_id")
          .agg(
            //first("prog_name").as("prog_name"),
            //first("start_date").as("start_date"),
            first("start_time").as("start_time"),
            first("start_date_time").as("start_date_time"),
            first("duration").as("duration"),
            count($"start_time").as("translations"))
          .where($"translations" === 1)

        //programs.show()

        def Diff = udf((one: String, two: String) => {
          one.toInt - two.toInt
        })

        def TimeDiff = udf((one: Timestamp, two: Timestamp) => {
          one.getTime - two.getTime
        })

        def TimeDiffStr = udf((one: Timestamp, two: Timestamp) => {
          val totalMinutes = TimeUnit.MILLISECONDS.toMinutes(one.getTime - two.getTime)
          val hours = totalMinutes / 60
          val minutes = totalMinutes % 60
          hours + ":" + minutes
        })

        def MilliToTime = udf((milliseconds: Long) => {
          val totalMinutes = TimeUnit.MILLISECONDS.toMinutes(milliseconds)
          val hours = totalMinutes / 60
          val minutes = totalMinutes % 60
          hours + ":" + minutes
        })

        def CompareMinutesPart = udf((one: String, two: String) => { // HHmmss
          one.substring(2) == two.substring(2)
        })

        println("#" * 30 + " " + cityname + " " + "#" * 30)

        val joined = programs.join(views, programs.col("prog_id") === views.col("prog_code"))
          //.withColumn("diff", Diff($"start_time", $"event_time"))
          .withColumn("diffSec", TimeDiff($"start_date_time", $"event_date_time"))
          .withColumn("diff", TimeDiffStr($"start_date_time", $"event_date_time"))
          .where($"diffSec" > 0 && CompareMinutesPart($"start_time", $"event_time"))

        joined.show(20)
        joined.describe("diffSec")
      }
    }
  }
}

// results:{
//  "Abilene-Sweetwater":6,
//  "Lake Charles":6,
//  "Seattle-Tacoma":8
// }

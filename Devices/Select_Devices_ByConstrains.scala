package SetTopBoxes.Devices

import java.text.SimpleDateFormat

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by yurav on 19/04/2017.
  */
object Select_Devices_ByConstrains {
  @transient val conf: SparkConf = new SparkConf().setMaster("local").setAppName("Devices")
  @transient val sc: SparkContext = new SparkContext(conf)

  case class ViewPeriod(
                         Date: Int,
                         ViewsCount: Int
                       ) extends Serializable

  val spark: SparkSession = SparkSession
    .builder()
    .master("local")
    .appName("Scala Spark SQL")
    .config("spark.sql.warehouse.dir", "file:///tmp/spark-warehouse")
    .getOrCreate()

  import spark.implicits._

  /** Main function */
  def main(args: Array[String]): Unit = {

    val cities = List[String]("Amarillo", "Parkersburg", "Little Rock-Pine Bluff", "Seattle-Tacoma")
    val format = new SimpleDateFormat("yyyyMMddHHmmss")
    val devices_appears_path = "s3n://magnet-fwm/home/TsviKuflik/y_devices/devices_appearances_by_views/part*"
    val devices_source = "s3n://magnet-fwm/home/TsviKuflik/y_devices/unique_devices_one_dev_in_house/*.gz"
    val startDate = 20150000
    val limitDateForDevicesPick = 20150701
    val minWatchDays = 120
    val minTotalViews = 2000
    val months = 5
    val results_path = "s3n://magnet-fwm/home/TsviKuflik/y_devices/" + months + "_months/"

    println("Loading devices from chosen cities with views appearance constrains")
    val appearsDT_Frame = sc.textFile(devices_appears_path)
      .map(x => {
        val firstComma = x.indexOf(",")
        val device_uniq_id = x.substring(0, firstComma)
        var views: Array[ViewPeriod] = null
        if (firstComma > 0 && x.length > (firstComma + 3)) {
          val viewsStrings = x.substring(firstComma + 1, x.length).split("\\|")
          views = viewsStrings.map(v => {
            val sp = v.split("\\:")
            ViewPeriod(sp(0).replace("-", "").toInt, sp(1).toInt)
          })
            .filter(v => v.ViewsCount > 0 && v.Date > startDate && v.Date < limitDateForDevicesPick)
        }
        (device_uniq_id, views)
      })
      .filter(d => d._2 != null && d._2.length > minWatchDays && d._2.map(v => v.ViewsCount).sum > minTotalViews)
      .toDF("device_id", "views")
      .drop("views")

    val houseHoldsWithOneDevice = spark.read
      .format("com.databricks.spark.csv")
      .load(devices_source)
      .select(
        $"_c1".as("deviceIdUniqe"),
        $"_c7".as("city"))

    val devicesOfCities = appearsDT_Frame.join(houseHoldsWithOneDevice,
      appearsDT_Frame.col("device_id") === houseHoldsWithOneDevice.col("deviceIdUniqe"))

    for (cityIndx <- cities.indices) {

      var city_name = cities(cityIndx)
      println("Saving " + city_name)
      devicesOfCities
        .filter($"city" === city_name)
        .drop($"deviceIdUniqe")
        .drop($"city")
        .coalesce(1)
        .write
        .format("com.databricks.spark.csv")
        .save(results_path + city_name.replace(" ", "-"))
    }

    println("done!")

  }
}

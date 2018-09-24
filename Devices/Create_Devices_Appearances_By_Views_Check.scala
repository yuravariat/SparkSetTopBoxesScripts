package SetTopBoxes.Devices

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
  * Created by yurav on 19/04/2017.
  */
object Create_Devices_Appearances_By_Views_Check {
  @transient val conf: SparkConf = new SparkConf().setMaster("local").setAppName("Devices")
  @transient val sc: SparkContext = new SparkContext(conf)

  //<editor-fold desc="Case classes">

  case class ViewPeriod(
               Date: Int,
               ViewsCount: Int
             ) extends Serializable

  //</editor-fold>

  /** Main function */
  def main(args: Array[String]): Unit = {

    sc.getConf.set("fs.s3n.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
    sc.getConf.set("fs.s3n.awsAccessKeyId", "")
    sc.getConf.set("fs.s3n.awsSecretAccessKey", "")


    val spark: SparkSession = SparkSession
      .builder()
      .master("local")
      .appName("Scala Spark SQL")
      .config("spark.sql.warehouse.dir", "file:///tmp/spark-warehouse")
      .getOrCreate()

    import spark.implicits._

    //<editor-fold desc="Run area">

    val devices_appears_path = "s3n://magnet-fwm/home/TsviKuflik/y_devices/devices_appearances_by_views/part*"
    val devices_source = "s3n://magnet-fwm/home/TsviKuflik/y_devices/unique_devices_one_dev_in_house/*.gz"

    val startDate: Int = 20150100
    val limitDate: Int = 20150701
    val daysToLearn: Int = 30
    val minWatchDays = 80
    val minTotalViews = 1000
    val dateLearnFrom = limitDate - (daysToLearn * 3.4).toInt

    println("startDate=" + startDate)
    println("limitDate=" + limitDate)
    println("daysToLearn=" + daysToLearn)
    println("minWatchDays=" + minWatchDays)
    println("minTotalViews=" + minTotalViews)
    println("dateLearnFrom=" + dateLearnFrom)

    val columns = Array("device_id", "views", "views_to_learn", "views_to_predict")
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
            .filter(v => v.ViewsCount > 0 && v.Date > startDate && v.Date < limitDate)
        }
        val views_to_learn = views.count(v => v.Date <= dateLearnFrom)
        val views_to_predict = views.count(v => v.Date > dateLearnFrom)
        (device_uniq_id, views, views_to_learn, views_to_predict)
      })
      .filter(d => d._2 != null && d._2.length > minWatchDays && d._2.map(v => v.ViewsCount).sum > minTotalViews)
      .toDF(columns: _*)

    //appearsDT_Frame.show()

    val houseHoldsWithOneDevice = spark.read
      .format("com.databricks.spark.csv")
      .load(devices_source)
      .select(
        //$"_c0".as("houseHoldID"),
        $"_c1".as("deviceIdUniqe"),
        //$"_c2".as("msoCode"),
        //$"_c3".as("mso"),
        //$"_c4".as("deviceID"),
        //$"_c5".as("householdType"),
        //$"_c6".as("zipcode"),
        $"_c7".as("city"))
        //$"_c8".as("cityCode"),
        //$"_c9".as("systemType"),
        //$"_c10".as("devicesCount"))

    //  def ArrayLength = udf((items: mutable.WrappedArray[Any]) => {
    //    if (items == null) false else items.nonEmpty
    //  })

    val joined = appearsDT_Frame.join(houseHoldsWithOneDevice,
      appearsDT_Frame.col("device_id") === houseHoldsWithOneDevice.col("deviceIdUniqe"))
      .groupBy("city")
      .agg(
        count($"device_id").as("devices"),
        count(when($"views_to_learn" > 0, 1)).as("devices_to_learn"),
        count(when($"views_to_predict" > 10, 1)).as("devices_to_predict")
      )
      .drop($"deviceIdUniqe")
      .orderBy(-$"devices")

    joined.show(120)

    //</editor-fold>

  }
}


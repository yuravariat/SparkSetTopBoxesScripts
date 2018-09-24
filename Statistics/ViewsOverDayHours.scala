package SetTopBoxes.Statistics

import java.net.URI
import java.text.SimpleDateFormat

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import java.util.concurrent.TimeUnit

import org.apache.hadoop.fs.{FileSystem, Path}

import scala.collection.mutable.ListBuffer

/**
  * Created by yurav on 19/04/2017.
  */
object ViewsOverDayHours {
  @transient val conf: SparkConf = new SparkConf().setMaster("local").setAppName("Devices")
  @transient val sc: SparkContext = new SparkContext(conf)

  val spark: SparkSession = SparkSession
    .builder()
    .master("local")
    .appName("Scala Spark SQL")
    .config("spark.sql.warehouse.dir", "file:///tmp/spark-warehouse")
    .getOrCreate()

  /** Main function */
  def main(args: Array[String]): Unit = {

    sc.getConf.set("fs.s3n.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
    sc.getConf.set("fs.s3n.awsAccessKeyId", "")
    sc.getConf.set("fs.s3n.awsSecretAccessKey", "")

    import org.apache.spark.sql.functions._
    import spark.implicits._

    //<editor-fold desc="Run area">

    println("\n" + "#" * 70 + " Views Over Day Hours " + "#" * 70 + "\n")

    val format = new SimpleDateFormat("yyyyMMddHHmmss")
    val formatHours = new SimpleDateFormat("HH")
    val limitDateForLearn = 20150601
    val limitDatePickViews = new java.sql.Timestamp(format.parse(limitDateForLearn.toString + "000000").getTime)
    val months = 5
    val chosen_devices_path = "s3n://magnet-fwm/home/TsviKuflik/y_devices/" + months + "_months/"
    val results_path_base = "s3n://magnet-fwm/home/TsviKuflik/y_stat/views_over_hours"
    val intervalsMinutes = 10
    val daysType = "weekday"

    def WeekdaysOrWeekend = udf((date: java.sql.Timestamp) => {
      import java.util.Locale
      val daynameFormat = new SimpleDateFormat("EEEE", Locale.US)
      val dayOfWeek = daynameFormat.format(date.getTime)
      if (dayOfWeek == "Saturday" || dayOfWeek == "Sunday") {
        "weekend"
      }
      else{
        "weekday"
      }
    })

    val cities = List[String]("Amarillo", "Parkersburg", "Little Rock-Pine Bluff", "Seattle-Tacoma")

    for (cityIndx <- cities.indices) {

      var city_name = cities(cityIndx).replace(" ", "-")
      val city_devices_path = chosen_devices_path + city_name + "/part-00000*"
      val deviceForThisCity = sc.textFile(city_devices_path).map(c => c.trim).collect()
      val results_path = results_path_base + "/" + city_name + (if (daysType.isEmpty) "" else "_" + daysType )
      println("Working on " + city_name + " " + deviceForThisCity.length)

      val viewsDF = spark.read
        .format("com.databricks.spark.csv")
        .load("s3n://magnet-fwm/home/TsviKuflik/y_views_with_durations/*.gz")
        .filter($"_c0" isin (deviceForThisCity: _*))
        .select(
          $"_c0".as("deviceIdUniqe"),
          $"_c1".cast("timestamp").as("time"),
          $"_c2".as("channel"),
          $"_c3".as("program"),
          $"_c4".cast("int").as("duration"),
          $"_c5".as("prog_duration"))
        .where($"time" <= limitDatePickViews && $"program" =!= "MP_MISSINGPROGRAM" && $"program" =!= "DVRPGMKEY" && $"duration" > 5)
        .withColumn("dayType", WeekdaysOrWeekend($"time"))
        .where($"dayType" === daysType)
        .flatMap(r => {
        val timeSlots: ListBuffer[(String, Int)] = ListBuffer()
        val startTime = r.getTimestamp(1)
        val intervals = (r.getInt(4) / intervalsMinutes) + (if (r.getInt(4) % intervalsMinutes != 0) 1 else 0)

        for (i <- 1 to intervals) {
          val minutes = (startTime.getMinutes / intervalsMinutes) * intervalsMinutes
          timeSlots.append((formatHours.format(startTime) + ":" + (if (minutes==0) "00" else minutes.toString), 1))
          startTime.setTime(startTime.getTime + TimeUnit.MINUTES.toMillis(intervalsMinutes))
        }

        timeSlots
      })
        .toDF("timeSlot", "viewsCount")
        .groupBy($"timeSlot")
        .agg(count("viewsCount").as("viewsCount"))
        .orderBy($"timeSlot")

      //val cached = viewsDF.cache
      //cached.show(100)

      if (FileSystem.get(new URI("s3n://magnet-fwm"), sc.hadoopConfiguration).exists(new Path(results_path))) {
        println("Deleting " + results_path)
        FileSystem.get(new URI("s3n://magnet-fwm"), sc.hadoopConfiguration).delete(new Path(results_path), true)
      }

      viewsDF.coalesce(1).write
        .format("com.databricks.spark.csv")
        .save(results_path)

      //viewsDF.createOrReplaceTempView(city_name.replace("-", "_") +"_views")

      //println("views count:" + viewsDF.count())
      //println("views count more than 5 minutes: " + viewsDF.where($"duration" > 5).count())
    }

    println("done!")

    //</editor-fold>
  }
}

package SetTopBoxes.Statistics

import java.net.URI
import java.text.SimpleDateFormat
import java.util.concurrent.TimeUnit

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.compress.GzipCodec
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

/**
  * Created by yurav on 19/04/2017.
  */
object ViewsOverDayHours_Test {
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

    println("\n" + "#" * 70 + " Views Over Day Hours Genres count " + "#" * 70 + "\n")

    val format = new SimpleDateFormat("yyyyMMddHHmmss")
    val formatHours = new SimpleDateFormat("HH")
    val limitDateForLearn = 20150601
    val limitDatePickViews = new java.sql.Timestamp(format.parse(limitDateForLearn.toString + "000000").getTime)
    val months = 5
    val base_path = "s3n://magnet-fwm/home/TsviKuflik/"
    val chosen_devices_path = base_path + "y_devices/" + months + "_months/"
    val all_prorams_path = base_path + "y_programs/programs_genres_groups/part*"
    val results_path_base = base_path + "y_stat/views_over_hours/"
    val intervalsMinutes = 10

    val programs = sc.textFile("s3n://magnet-fwm/home/TsviKuflik/y_programs/programs_genres_groups/part*")
      .flatMap(s => {
        val sp = s.split("\\|")
        val prog_name = sp(0)
        val genres = if (sp(1).isEmpty) null else sp(1).split(",")
        val ids = ListBuffer[String]()
        for (i <- 2 until sp.length) {
          ids.append(sp(i))
        }
        ids.map(id => (id, genres))
      })
      .toDF("prog_id", "genres")
    //.collect()
    //.toMap

    def PartOfDay = udf((date: java.sql.Timestamp) => {
      val hour = TimeUnit.MILLISECONDS.toHours(date.getTime) % 24
      if (hour >= 6 && hour < 11) {
        1
      }
      else if (hour >= 11 && hour < 14) {
        2
      }
      else if (hour >= 14 && hour < 20) {
        3
      }
      else if (hour >= 20 || hour < 2) {
        4
      }
      else {
        5
      }
    })

    def WeekdaysOrWeekend = udf((date: java.sql.Timestamp) => {
      import java.util.Locale
      val daynameFormat = new SimpleDateFormat("EEEE", Locale.US)
      val dayOfWeek = daynameFormat.format(date.getTime)
      if (dayOfWeek == "Saturday" || dayOfWeek == "Sunday") {
        "weekend"
      }
      else {
        "weekday"
      }
    })

    val cities = List[String]("Amarillo", "Parkersburg", "Little Rock-Pine Bluff", "Seattle-Tacoma")

    for (cityIndx <- cities.indices) {

      var city_name = cities(cityIndx).replace(" ", "-")
      val city_devices_path = chosen_devices_path + city_name + "/part-00000*"
      val deviceForThisCity = sc.textFile(city_devices_path).map(c => c.trim).collect()
      val results_path = results_path_base + "/" + city_name + "_genres_stat"
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
        .withColumn("daypart", PartOfDay($"time"))
        .withColumn("dayType", WeekdaysOrWeekend($"time"))
        .join(programs, $"program"===$"prog_id")
      //.join(programs,programs.col($"progam") == viewsDF.col($"proram"))

      //val joined = viewsDF.join(programs,$"progam" === $"proram")

      //viewsDF.show()

      val countRDD = viewsDF.rdd
        .filter(r => !r.isNullAt(9))
        .flatMap(r => {
          val daypart = r.getInt(6)
          val dayType = r.getString(7)
          r.getSeq[String](9).map(s => (dayType + "_" + daypart + "_" + s, 1))
        }
        )
        .reduceByKey(_ + _)
        .sortByKey()
        .map(d => d._1.split("_").mkString(",") + "," + d._2)

      if (FileSystem.get(new URI("s3n://magnet-fwm"), sc.hadoopConfiguration).exists(new Path(results_path))) {
        println("Deleting " + results_path)
        FileSystem.get(new URI("s3n://magnet-fwm"), sc.hadoopConfiguration).delete(new Path(results_path), true)
      }

      countRDD.coalesce(1).saveAsTextFile(results_path, classOf[GzipCodec])
    }

    println("done!")

    //</editor-fold>
  }
}

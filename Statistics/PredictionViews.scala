package SetTopBoxes.Statistics

import java.io.Serializable
import java.net.URI
import java.text.SimpleDateFormat

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

//<editor-fold desc="Case classes">

case class ViewingData(
  device_id: String,
  event_date_time: java.sql.Timestamp,
  //station_num: String,
  prog_code: String,
  duration: Int
  //prog_duration: Int
) extends Serializable

//</editor-fold>

object PredictionsTopGenres {
  @transient val conf: SparkConf = new SparkConf().setMaster("local").setAppName("Devices")
  @transient val sc: SparkContext = new SparkContext(conf)
  val spark: SparkSession = SparkSession
    .builder()
    .master("local")
    .appName("Scala Spark SQL")
    .config("spark.sql.warehouse.dir", "file:///tmp/spark-warehouse")
    .getOrCreate()

  import spark.implicits._

  def main(args: Array[String]): Unit = {

    //<editor-fold desc="Run area">

    println("\n" + "#" * 70 + " Predictions views " + "#" * 70 + "\n")

    var basePath = "s3n://magnet-fwm/home/TsviKuflik/"
    val format = new SimpleDateFormat("yyyyMMddHHmmss")
    val notAllowedPRograms = Array[String]("MP_MISSINGPROGRAM", "DVRPGMKEY"
      , "EP000009937449", "SH000191120000", "SH000000010000", "H002786110000", "SH015815970000", "SH000191160000"
      , "SH000191300000", "SH000191680000", "SH001083440000", "SH003469020000", "SH007919190000", "SH008073210000"
      , "SH014947440000", "SH016938330000", "SH018049520000", "SH020632040000", "SP003015230000")

    val programs_path = basePath + "y_programs/programs_genres_groups/part-00000"

    val prograrms = sc.textFile(programs_path)
      .zipWithIndex
      .map { case (s, index) =>
        val sp = s.split("\\|")
        val genres = sp(1)
        val codes = sp.slice(2, sp.length)
        (sp(0),genres,codes)
      }
      .collect()

    val programsWithGenres = prograrms.filter(g =>g._2!=null && !g._2.isEmpty)

    println("Total programs " + prograrms.length)
    println("Total programs with genres " + programsWithGenres.length)
    println(" ")

    val cities = List("Amarillo", "Parkersburg", "Little Rock-Pine Bluff", "Seattle-Tacoma")

    for (city_name <- cities) {

      val city_name_safe = city_name.replace(" ", "-")
      val devices_views_path = basePath + "y_for_prediction/views/5_months/" + city_name_safe + "-0601-0608/*.gz"

      val devicesViews = sc.textFile(devices_views_path)
        .flatMap(s => {
          val splited = s.split(",").toList
          val device_id = splited.head
          val views = splited.slice(1, s.length)
            .map(v => {
              val sp = v.split("\\|")
              val dt = new java.sql.Timestamp(format.parse(sp(0)).getTime)
              ViewingData(device_id=device_id, event_date_time = dt, prog_code = sp(1), duration = sp(2).toInt)
            })
          views
        })

      println(city_name + " Total views " + devicesViews.count())
      println(city_name + " Total views with genres " + devicesViews.filter(v =>programsWithGenres.exists(p=>p._3.contains(v.prog_code))).count())
      println(city_name + " Total views notAllowedPRograms " + devicesViews.filter(v =>notAllowedPRograms.contains(v.prog_code)).count())
      println(" ")
    }

    println("Done!")

    //</editor-fold>
  }
}

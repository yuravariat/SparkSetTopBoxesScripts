package SetTopBoxes.Sequences

import java.net.URI
import java.text.SimpleDateFormat
import java.util.Calendar
import java.util.concurrent.TimeUnit

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.compress.GzipCodec
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
  * Created by yurav on 19/04/2017.
  */
object CreateSequencesStep0 {

  val spark: SparkSession = SparkSession
    .builder()
    .master("local")
    .appName("Scala Spark SQL")
    .config("spark.sql.warehouse.dir", "file:///tmp/spark-warehouse")
    .getOrCreate()

  import spark.implicits._

  //<editor-fold desc="Classes">

  case class ViewingData(
                          device_id_unique: String,
                          event_date_time: java.sql.Timestamp,
                          station_num: String,
                          prog_code: String,
                          var duration: Int,
                          prog_duration: Int
                        ) extends Serializable{
    override def toString: String =
      s"$device_id_unique|$event_date_time|$prog_code|$duration|$prog_duration"
  }

  case class SequenceItem(
                           device_id_unique: String,
                           event_date_time: java.sql.Timestamp,
                           rated_genres: List[(String, Int)]
                         ) extends Serializable {
    override def toString: String =
      s"$device_id_unique|$event_date_time|${rated_genres.map(t => t._1 + "->" + t._2).mkString(",")}"
  }

  //</editor-fold>

  /** Main function */
  def main(args: Array[String]): Unit = {
    @transient val conf: SparkConf = new SparkConf().setMaster("local").setAppName("Devices")
    @transient val sc: SparkContext = new SparkContext(conf)

    //<editor-fold desc="Run area">

    /*
    Script that collects views from y_views_with_durations and store it in other location.
     Views of devices from 4 regions. Cleaning duration<5 minutes, MP_MISSINGPROGRAM ...
    result example:
    17719-00000099cb1b|2015-01-01 00:04:53.0|EP013320550428|25|36
    17719-00000099cb1b|2015-01-01 00:30:00.0|EP013320550429|30|36
    17719-00000099cb1b|2015-01-01 01:00:00.0|EP012538160211|30|30
    */

    println("\n" + "#" * 70 + " CreateSequences Step 0 " + "#" * 70 + "\n")

    val base_path = "s3n://magnet-fwm/home/TsviKuflik/"

    val cities = List[String]("Amarillo", "Parkersburg", "Little Rock-Pine Bluff", "Seattle-Tacoma")
    var devices_views_path = List(base_path + "y_views_with_durations/*.gz")

    val format = new SimpleDateFormat("yyyyMMddHHmmss")
    val formatMonthDay = new SimpleDateFormat("MMdd")

    val start = new java.sql.Timestamp(format.parse("20150101000000").getTime)
    val end = new java.sql.Timestamp(format.parse("20150615000000").getTime)
    val months = 5
    val chosen_devices_path = base_path + "y_devices/" + months + "_months/"

    println("cities=" + cities.mkString(","))
    println("start=" + start)
    println("end=" + end)

    //<editor-fold desc="Create programs map">
    println("Creating programs map")
    val notAllowedPrograms = List("MP_MISSINGPROGRAM", "DVRPGMKEY"
      , "EP000009937449", "SH000191120000", "SH000000010000", "H002786110000", "SH015815970000", "SH000191160000"
      , "SH000191300000", "SH000191680000", "SH001083440000", "SH003469020000", "SH007919190000", "SH008073210000"
      , "SH014947440000", "SH016938330000", "SH018049520000", "SH020632040000", "SP003015230000")

    println("Creating programs map end")
    //</editor-fold>

    //<editor-fold desc="Create Sequences for each city">

    println("Create Sequences for each city")
    for (cityIndx <- cities.indices) {

      var city_name = cities(cityIndx).replace(" ", "-")
      val city_devices_path = chosen_devices_path + city_name + "/part-00000*"
      val deviceForThisCity = sc.textFile(city_devices_path).map(c => c.trim).collect()
      println("Working on " + city_name + " " + deviceForThisCity.length)

      val results_path = base_path + "y_sequences/step0/" + city_name + "/" +
        formatMonthDay.format(start) + "-" + formatMonthDay.format(end)
      println("Output to " + results_path)

      val devicesViews = spark.read
        .format("com.databricks.spark.csv")
        .load(devices_views_path: _*)
        .filter($"_c0" isin (deviceForThisCity: _*))
        .select(
          $"_c0", // device_id
          $"_c1".cast("timestamp"), // date
          $"_c2", // channel number
          $"_c3", // program code
          $"_c4".cast("int"), // duration
          $"_c5".cast("int") // prog_duration
        )
        .where($"_c1" > start && $"_c1" < end && $"_c3" =!= "MP_MISSINGPROGRAM" && $"_c3" =!= "DVRPGMKEY"
          && !$"_c3".isin(notAllowedPrograms: _*) && $"_c4" > 5)
        .rdd
        .map(r =>
          ViewingData(
            r.getString(0),
            r.getTimestamp(1),
            r.getString(2),
            r.getString(3),
            r.getInt(4),
            r.getInt(5))
        )

      if (FileSystem.get(new URI("s3n://magnet-fwm"), sc.hadoopConfiguration).exists(new Path(results_path))) {
        println("Deleting " + results_path)
        FileSystem.get(new URI("s3n://magnet-fwm"), sc.hadoopConfiguration).delete(new Path(results_path), true)
      }

      devicesViews
        .saveAsTextFile(results_path, classOf[GzipCodec])
    }

    //</editor-fold>

    println("done!")

    //</editor-fold>
  }
}

// results:{
//  "Abilene-Sweetwater":6,
//  "Lake Charles":6,
//  "Seattle-Tacoma":8
// }

// Remove suspicies programs :
// EP000009937449|College Football|Sports event,Football,Playoff sports|20150102|060000|120.0
// SH000191120000|SIGN OFF|Special|20150101|060000|480.0
// SH000000010000|Paid Programming|Shopping|20150331|000000|300.0
// H002786110000|CountyNet||20150102|140000|300.0
// SH015815970000|ViaCast TV|Variety|20150101|050000|300.0
// SH000191160000|ABC World News Now|News|20150101|071300|107.0
// SH000191300000|Up to the Minute|News|20150101|070000|120.0
// SH000191680000|To Be Announced|Special|20150101|043500|122.0
// SH001083440000|Classic Arts Showcase|Art|20150102|080000|300.0
// SH003469020000|PA Public Affairs|Public affairs|20150102|050000|420.0
// SH007919190000|Blazers Television Off-Air||20141231|200000|360.0
// SH008073210000|Movie|Special,Entertainment|20141231|230000|120.0
// SH014947440000|Weather Nation|Weather|20150102|140000|360.0
// SH016938330000|Película|Special,Entertainment|20150101|234500|110.0
// SH018049520000|24 Hour Local Weather & Information|Weather,Community|20150102|010000|240.0
// SH020632040000|¡Feliz 2015!|Special,Entertainment|20150101|010000|420.0
// SP003015230000|NHL Hockey|Sports event,Hockey|20150102|150000|180.0

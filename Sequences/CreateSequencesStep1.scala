package SetTopBoxes.Sequences

import java.io.Serializable
import java.net.URI
import java.text.SimpleDateFormat
import java.util.Calendar
import java.util.concurrent.TimeUnit

import org.apache.commons.net.ntp.TimeStamp
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.compress.GzipCodec
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.udf
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
  * Created by yurav on 19/04/2017.
  */
object CreateSequencesStep1 {

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
                        ) extends Serializable

  case class Program(
                      prog_code: String,
                      duration: Int,
                      total_duration: Int,
                      view_start: java.sql.Timestamp,
                      genres: Array[String]
                    ) extends Serializable

  case class SequenceItem(
                           device_id_unique: String,
                           event_date_time: java.sql.Timestamp,
                           programs: List[Program],
                           rated_genres: List[(String, Double)]
                         ) extends Serializable {

    override def toString: String =
      s"$device_id_unique|" +
        s"$event_date_time|" +
        s"[${
          programs.map(p => "{\"code\":\"" + p.prog_code + "\",\"dur\":" + p.duration + ",\"total_dur\":" + p.total_duration +
            ",\"view_start\":\"" + p.view_start + "\",\"genres\":[" + p.genres.map(s => "\"" + s + "\"").mkString(",") + "]}").mkString(",")
        }]|" +
        s"${rated_genres.map(t => t._1 + "->" + t._2).mkString(",")}"
  }

  //</editor-fold>

  /** Main function */
  def main(args: Array[String]): Unit = {
    @transient val conf: SparkConf = new SparkConf().setMaster("local").setAppName("Devices")
    @transient val sc: SparkContext = new SparkContext(conf)

    //<editor-fold desc="Run area">

    /*
    Script that groups views by time slots (15 minutes for example)
     Result is device, time slots and collection of viewed programs in this slot in addition wiegths of each genre in this time slot.
    result example:
    17719-0000000abf27|2015-01-01 09:00:00.0|[{"code":"SH009042580000","dur":13,"total_dur":13,"view_start":"2015-01-01 09:00:00.0","genres":["Talk","News"]}]|Talk->0.8666666666666667,News->0.8666666666666667
    17719-0000000abf27|2015-01-01 09:15:00.0|[{"code":"SH009042580000","dur":10,"total_dur":16,"view_start":"2015-01-01 09:20:39.0","genres":["Talk","News"]}]|Talk->0.6666666666666666,News->0.6666666666666666
    17719-0000000abf27|2015-01-01 09:30:00.0|[{"code":"SH009042580000","dur":6,"total_dur":16,"view_start":"2015-01-01 09:20:39.0","genres":["Talk","News"]}]|Talk->0.4,News->0.4
    */

    println("\n" + "#" * 70 + " CreateSequences Step 1 " + "#" * 70 + "\n")

    val base_path = "s3n://magnet-fwm/home/TsviKuflik/"

    val cities = List[String]("Amarillo", "Parkersburg", "Little Rock-Pine Bluff", "Seattle-Tacoma")

    val format = new SimpleDateFormat("yyyyMMddHHmmss")
    val formatMonthDay = new SimpleDateFormat("MMdd")
    val timeStampFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")

    val start = new java.sql.Timestamp(format.parse("20150101000000").getTime)
    val end = new java.sql.Timestamp(format.parse("20150615000000").getTime)
    val slotMinutes = 15

    println("cities=" + cities.mkString(","))
    println("start=" + start)
    println("end=" + end)

    //<editor-fold desc="Create programs map">
    println("Creating programs map")

    val GenresArray = sc.textFile(base_path + "y_programs/genres/part*")
      .map(_.split("\\|"))
      .filter(s => !s(0).isEmpty)
      .map(s => s(0))
      .collect()
      .sortBy(s => s)
      .toList

    val programs = sc.textFile(base_path + "y_programs/programs_genres_groups/part*")
      .map(s => {
        val sp = s.split("\\|")
        val prog_name = sp(0)
        val genres = sp(1)
        val ids = ListBuffer[String]()
        for (i <- 2 until sp.length) {
          ids.append(sp(i))
        }
        (prog_name, genres, ids)
      })
      .sortBy(_._1)

    val progsToGenresArray = programs
      .filter(p => {
        p._2.!=(null) && !p._2.isEmpty
      })
      .flatMap({ p =>
        p._3.map(id => (id, p._2.split(",")))
      })
      .collect()
      .toMap

    println("Creating programs map end")
    //</editor-fold>

    //<editor-fold desc="Create Sequences for each city">

    println("Create Sequences for each city")
    for (cityIndx <- cities.indices) {

      var city_name = cities(cityIndx).replace(" ", "-")
      var devices_views_path = base_path + "y_sequences/step0/" + city_name + "/" +
        formatMonthDay.format(start) + "-" + formatMonthDay.format(end) + "/*.gz"

      println("Working on " + city_name)

      val results_path = base_path + "y_sequences/step1/" + city_name + "/" +
        formatMonthDay.format(start) + "-" + formatMonthDay.format(end)
      println("Output to " + results_path)

      val devicesViews = sc.textFile(devices_views_path)
        .map(s => {
          val sp = s.split("\\|")
          val dt = new java.sql.Timestamp(timeStampFormat.parse(sp(1)).getTime)
          ViewingData(sp(0), dt, "", sp(2), sp(3).toInt, sp(4).toInt)
        })
        .filter(v => {
          (v.event_date_time.getTime > start.getTime && v.event_date_time.getTime < end.getTime)
        })
        .map(v => (v.device_id_unique, v))
        .groupBy(r => r._1)
        .flatMap(d => {
          val device_id = d._1
          val views = d._2.flatMap(d => {
            val cal = Calendar.getInstance
            cal.setTime(d._2.event_date_time)
            val slotNum = cal.get(java.util.Calendar.MINUTE) / slotMinutes

            var rounded_time = new java.sql.Timestamp(d._2.event_date_time.getTime
              - TimeUnit.MINUTES.toMillis(cal.get(java.util.Calendar.MINUTE))
              - TimeUnit.SECONDS.toMillis(cal.get(java.util.Calendar.SECOND))
              - TimeUnit.MILLISECONDS.toMillis(cal.get(java.util.Calendar.MILLISECOND))
              + TimeUnit.MINUTES.toMillis(slotNum * slotMinutes))

            val slots = ListBuffer[(java.sql.Timestamp, Program)]()

            var _total_duration = d._2.duration
            var durationInThisSlot = (slotMinutes * (slotNum + 1)) - cal.get(java.util.Calendar.MINUTE)
            if (_total_duration < slotMinutes && _total_duration < durationInThisSlot) {
              durationInThisSlot = _total_duration
            }
            val genresArrayOfThisProg: Array[String] =
              if (progsToGenresArray.contains(d._2.prog_code)) progsToGenresArray(d._2.prog_code) else Array[String]("nogenre")

            slots.append(
              (rounded_time, Program(d._2.prog_code, durationInThisSlot, d._2.duration, d._2.event_date_time, genresArrayOfThisProg))
            )
            _total_duration -= durationInThisSlot

            var count = 1
            while (_total_duration > 0) {
              val time = new java.sql.Timestamp(rounded_time.getTime + count * TimeUnit.MINUTES.toMillis(slotMinutes))
              durationInThisSlot = if (_total_duration > slotMinutes) slotMinutes else _total_duration
              slots.append(
                (time, Program(d._2.prog_code, durationInThisSlot, d._2.duration, d._2.event_date_time, genresArrayOfThisProg))
              )
              _total_duration -= slotMinutes
              count += 1
            }

            slots
          })

          val grouped_by_time = views.groupBy(_._1).map(g => {
            val bufferMap: mutable.Map[String, Double] = mutable.Map[String, Double]()
            val programs = g._2.map(f => f._2).toList
            for (dt <- g._2) {
              for (genre <- dt._2.genres) {
                val weigth = if (genre == "nogenre") 0 else dt._2.duration / slotMinutes.toDouble
                if (bufferMap.contains(genre)) {
                  bufferMap(genre) = bufferMap(genre) + weigth
                }
                else {
                  bufferMap += (genre -> weigth)
                }
              }
            }
            (g._1, programs, bufferMap.toList.sortBy(-_._2))
          })

          grouped_by_time.map(t => SequenceItem(device_id, t._1, t._2, t._3))
        })
        .sortBy(v => (v.device_id_unique, v.event_date_time.getTime))

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

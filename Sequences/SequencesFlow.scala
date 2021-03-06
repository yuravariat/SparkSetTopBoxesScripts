package SetTopBoxes.Sequences

import java.io.Serializable
import java.net.URI
import java.text.SimpleDateFormat
import java.util.Calendar
import java.util.concurrent.TimeUnit

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.compress.GzipCodec
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.mortbay.util.ajax.JSON

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

//<editor-fold desc="Classes">

case class ViewsSlot(
                      device_id_unique: String,
                      event_date_time: java.sql.Timestamp,
                      genre: String
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
                         rated_genres: List[(String, Double)],
                         main_genre: String
                       ) extends Serializable

//</editor-fold>

/**
  * Created by yurav on 19/04/2017.
  */
object SequencesFlow {

  val spark: SparkSession = SparkSession
    .builder()
    .master("local")
    .appName("Scala Spark SQL")
    .config("spark.sql.warehouse.dir", "file:///tmp/spark-warehouse")
    .getOrCreate()

  /** Main function */
  def main(args: Array[String]): Unit = {
    @transient val conf: SparkConf = new SparkConf().setMaster("local").setAppName("Devices")
    @transient val sc: SparkContext = new SparkContext(conf)

    import spark.implicits._

    //<editor-fold desc="Run area">

    println("\n" + "#" * 70 + " Sequences Flow " + "#" * 70 + "\n")

    val base_path = "s3n://magnet-fwm/home/TsviKuflik/"

    val format = new SimpleDateFormat("yyyyMMddHHmmss")
    val formatMonthDay = new SimpleDateFormat("MMdd")
    val formatDate = new SimpleDateFormat("yyyy-MM-dd")
    val timeStampFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")

    val start = new java.sql.Timestamp(format.parse("20150101000000").getTime)
    val end = new java.sql.Timestamp(format.parse("20150615000000").getTime)
    val months = 5
    val chosen_devices_path = base_path + "y_devices/" + months + "_months/"
    val slotMinutes = 15
    val city = "Amarillo"

    val city_name = city.replace(" ", "-")
    val path = base_path + "y_sequences/step1/" + city_name + "/" +
      formatMonthDay.format(start) + "-" + formatMonthDay.format(end) + "/*.gz"
    val city_devices_path = chosen_devices_path + city_name + "/part-00000*"

    val deviceForThisCity = sc.textFile(city_devices_path).map(c => c.trim).take(10)
    println("start=" + start)
    println("end=" + end)
    println("city=" + city)
    println("path=" + path)
    println("\n" + "#" * 200 + "\n")

    for (device_id <- deviceForThisCity) {

      val results_path = base_path + "y_sequences/step10/" + city_name + "/" + device_id

      println("device_id=" + device_id)
      println("results_path=" + results_path)

      val sequences = sc.textFile(path)
        .filter(_.startsWith(device_id))
        .map(s => {
          val sp = s.split("\\|")
          val dt = new java.sql.Timestamp(timeStampFormat.parse(sp(1)).getTime)
          val rated_genres = sp(3).split(",").map(s => {
            val _sp = s.split("->")
            (_sp(0), _sp(1).toDouble)
          }).sortBy(-_._2).toList

          SequenceItem(sp(0), dt, rated_genres, rated_genres.head._1)
        })

      //sequences.toDS().createOrReplaceTempView("sequences")

      var grouped = sequences.groupBy(s => formatDate.format(s.event_date_time))
        .map(g => {
          val cal = Calendar.getInstance
          var counter = 0
          var arrIndex = 0
          val genresArray = g._2.toList.sortBy(_.event_date_time.getTime)
          val genres = ListBuffer[String]()

          val slots_in_day = 60 * 24 / slotMinutes
          while (counter < slots_in_day * slotMinutes) {
            if (arrIndex < genresArray.length) {
              cal.setTime(genresArray(arrIndex).event_date_time)
              val hours = cal.get(java.util.Calendar.HOUR_OF_DAY)
              val minutes = cal.get(java.util.Calendar.MINUTE)

              // If it is the same hour and same minutes.
              if (hours == counter / 60 && minutes == counter % 60) {
                //genres.append(s"$hours:$minutes - ${genresArray(arrIndex).main_genre}")
                genres.append(s"${genresArray(arrIndex).main_genre}")
                arrIndex += 1
              }
              else {
                //genres.append(s"${counter / 60}:${counter % 60} - none")
                genres.append("-")
              }
            }
            else {
              //genres.append(s"${counter / 60}:${counter % 60} - none")
              genres.append("-")
            }
            counter += slotMinutes
          }

          (g._1, genres.toList)

        })

      val keys = grouped.map(_._1).collect()
      val missing_keys = ListBuffer[(String, List[String])]()
      val cal = Calendar.getInstance
      cal.setTime(start)

      while (cal.getTime.getTime < end.getTime) {
        if (!keys.contains(formatDate.format(cal.getTime))) {
          missing_keys.append((formatDate.format(cal.getTime), List.fill(96)("-")))
        }
        cal.setTime(new java.sql.Timestamp(cal.getTime.getTime + TimeUnit.DAYS.toMillis(1)))
      }

      if(missing_keys.nonEmpty) {
        grouped = grouped.union(sc.parallelize(missing_keys))
      }

      if (FileSystem.get(new URI("s3n://magnet-fwm"), sc.hadoopConfiguration).exists(new Path(results_path))) {
        println("Deleting " + results_path)
        FileSystem.get(new URI("s3n://magnet-fwm"), sc.hadoopConfiguration).delete(new Path(results_path), true)
      }

      grouped
        .coalesce(1)
        .sortBy(_._1)
        .map(v => v._1 + "," + v._2.mkString(","))
        .saveAsTextFile(results_path)
    }

    println("\n" + "#" * 200 + "\n")
    println("done!")

    //</editor-fold>


    //</editor-fold>
  }
}

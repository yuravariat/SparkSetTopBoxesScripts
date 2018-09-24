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

import scala.collection.mutable.ListBuffer
import scala.util.parsing.json._

/**
  * Created by yurav on 19/04/2017.
  */
object CreateSequencesStep2_progs {

  val spark: SparkSession = SparkSession
    .builder()
    .master("local")
    .appName("Scala Spark SQL")
    .config("spark.sql.warehouse.dir", "file:///tmp/spark-warehouse")
    .getOrCreate()

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

  case class Prog(
                   index: Long,
                   codes: Seq[String]
                 ) extends Serializable

  case class SequenceItem(
                           device_id_unique: String,
                           event_date_time: java.sql.Timestamp,
                           rated_genres: List[(String, Double)],
                           main_genre: String
                         ) extends Serializable

  //</editor-fold>

  /** Main function */
  def main(args: Array[String]): Unit = {
    @transient val conf: SparkConf = new SparkConf().setMaster("local").setAppName("Devices")
    @transient val sc: SparkContext = new SparkContext(conf)

    //<editor-fold desc="Run area">

    /*
    Script that creates matrix where columns are time slots rows are days.
     In each cell appears most dominant genre '-' if no views and nogenre there were views but without genres.
    result example:
    17719-000002cc1763,2015-02-25,Talk,Talk,Talk,Talk,Talk,Talk,Talk,Talk,Talk,Talk,Talk,Talk,Talk,Talk,
    17719-000002cc1763,2015-02-14,-,-,-,-,-,-,-,-,-,-,-,-,-,-,-,-,-,-,-,-,-,-,-,-,-,-,-,-,-,-,-,-,-,-,-,
    17719-000002cc1763,2015-03-19,Talk,-,-,-,-,-,-,-,-,-,-,-,-,-,-,-,News,News,News,News,News,News,News,
    17719-000002cc1763,2015-03-18,Talk,Talk,Talk,Talk,Talk,Talk,Talk,Talk,Talk,Talk,Talk,Talk,Talk,Talk,
    17719-000002cc1763,2015-05-22,Talk,Talk,Talk,Talk,Talk,Talk,Talk,Talk,Talk,Talk,Talk,Talk,Talk,Talk,
    */

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
    val cities = List[String]("Amarillo", "Parkersburg", "Little Rock-Pine Bluff", "Seattle-Tacoma")
    val programs_path = base_path + "y_programs/programs_groups/part-00000"

    val prograrms = sc.textFile(programs_path)
      .zipWithIndex
      .map { case (s, index) =>
        val sp = s.split("\\|")
        //(index, sp(0), sp.slice(1, sp.length)) //(index, name, programs codes)
        Prog(index, sp.slice(1, sp.length))
      }
      .collect()

    // Program code to prog index dict
    val programsCodesToIndex: Map[String, Long] = prograrms
      .flatMap { p => p.codes.map(t => (t, p.index)) } // (program code,program index in programsVector )
      .toMap

    for (city <- cities) {

      val city_name = city.replace(" ", "-")
      val path = base_path + "y_sequences/step1/" + city_name + "/" +
        formatMonthDay.format(start) + "-" + formatMonthDay.format(end) + "/*.gz"

      println("city=" + city)
      println("path=" + path)
      val results_path = base_path + "y_sequences/step2_progs/" + city_name + "/" +
        formatMonthDay.format(start) + "-" + formatMonthDay.format(end)

      println("results_path=" + results_path)
      println("\n" + "#" * 200 + "\n")

      val sequences = sc.textFile(path)
        .map(s => {
          val sp = s.split("\\|")
          val dt = new java.sql.Timestamp(timeStampFormat.parse(sp(1)).getTime)
          val _rated_progs = ListBuffer[(String, Double)]()
          try{
            val jsonString = sp(2).stripMargin
            val jsonObject = JSON.parseFull(jsonString).get.asInstanceOf[List[Map[String,Any]]]
            for(prog <- jsonObject){
              _rated_progs.append((prog("code").asInstanceOf[String], prog("dur").asInstanceOf[Double]))
            }
          } catch{
            case e: Exception => print("nothing")
          }
          val rated_progs: Seq[(String, Double)] = _rated_progs.sortBy(-_._2).toList
          SequenceItem(sp(0), dt, rated_progs.toList, rated_progs.head._1 + ":" + programsCodesToIndex(rated_progs.head._1).toString)
        })

      //sequences.toDS().createOrReplaceTempView("sequences")

      var grouped_rdd = sequences
        .groupBy(_.device_id_unique)
        .map(d => {
          var grouped_by_day = d._2.groupBy(v => formatDate.format(v.event_date_time))
            .map(g => {
              val cal = Calendar.getInstance
              var counter = 0
              var arrIndex = 0
              val progsArray = g._2.toList.sortBy(_.event_date_time.getTime)
              val progs = ListBuffer[String]()

              val slots_in_day = 60 * 24 / slotMinutes
              while (counter < slots_in_day * slotMinutes) {
                if (arrIndex < progsArray.length) {
                  cal.setTime(progsArray(arrIndex).event_date_time)
                  val hours = cal.get(java.util.Calendar.HOUR_OF_DAY)
                  val minutes = cal.get(java.util.Calendar.MINUTE)

                  // If it is the same hour and same minutes.
                  if (hours == counter / 60 && minutes == counter % 60) {
                    //genres.append(s"$hours:$minutes - ${genresArray(arrIndex).main_genre}")
                    progs.append(s"${progsArray(arrIndex).main_genre}")
                    arrIndex += 1
                  }
                  else {
                    //genres.append(s"${counter / 60}:${counter % 60} - none")
                    progs.append("-")
                  }
                }
                else {
                  //genres.append(s"${counter / 60}:${counter % 60} - none")
                  progs.append("-")
                }

                counter += slotMinutes
              }

              (g._1, progs.toList)
            })

          val missing_keys = ListBuffer[(String, List[String])]()
          val cal = Calendar.getInstance
          cal.setTime(start)

          while (cal.getTime.getTime < end.getTime) {
            if (!grouped_by_day.contains(formatDate.format(cal.getTime))) {
              missing_keys.append((formatDate.format(cal.getTime), List.fill(96)("-")))
            }
            cal.setTime(new java.sql.Timestamp(cal.getTime.getTime + TimeUnit.DAYS.toMillis(1)))
          }

          if (missing_keys.nonEmpty) {
            grouped_by_day ++= missing_keys
          }

          (d._1, grouped_by_day)
        })
        .flatMap(d => d._2.map(m => (d._1, m._1, m._2)))

      if (FileSystem.get(new URI("s3n://magnet-fwm"), sc.hadoopConfiguration).exists(new Path(results_path))) {
        println("Deleting " + results_path)
        FileSystem.get(new URI("s3n://magnet-fwm"), sc.hadoopConfiguration).delete(new Path(results_path), true)
      }

      grouped_rdd
        .map(d => d._1 + "," + d._2 + "," + d._3.mkString(","))
        .saveAsTextFile(results_path, classOf[GzipCodec])
    }
    println("\n" + "#" * 200 + "\n")
    println("done!")

    //</editor-fold>
  }
}

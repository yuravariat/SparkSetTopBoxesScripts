package SetTopBoxes.Prediction

import java.io.Serializable
import java.net.URI
import java.text.SimpleDateFormat
import java.util.concurrent.TimeUnit

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.compress.GzipCodec
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

//<editor-fold desc="Case classes">

case class ViewingData(
                        event_date_time: java.sql.Timestamp,
                        prog_code: String,
                        duration: Int) extends Serializable {

  override def toString: String = {
    val dt = new java.text.SimpleDateFormat("yyyyMMddHHmmss").format(event_date_time)
    s"$dt|$prog_code|$duration"
  }
}

case class Log(device_id: String,
               event_date: java.sql.Timestamp,
               _type: String,
               value: String,
               name: String,
               event_id: String) extends Serializable {
}

//</editor-fold>

/**
  * Created by yurav on 19/04/2017.
  */
object CreateViewsForPredictionOnTuning {

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

    val spark: SparkSession = SparkSession
      .builder()
      .master("local")
      .appName("Scala Spark SQL")
      .config("spark.sql.warehouse.dir", "file:///tmp/spark-warehouse")
      .getOrCreate()

    //<editor-fold desc="Run area">

    println("\n" + "#" * 70 + " CreateViewsForPredictionOnTuning " + "#" * 70 + "\n")

    val cities = List("Amarillo", "Parkersburg", "Little Rock-Pine Bluff", "Seattle-Tacoma")
    val format = new SimpleDateFormat("yyyyMMddHHmmss")
    var formatUTC = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SS'Z'")
    val formatMonthDay = new SimpleDateFormat("MMdd")
    val base_path = "s3n://magnet-fwm/home/TsviKuflik/"
    val predictionStart = new java.sql.Timestamp(format.parse("20150601000000").getTime)
    val predictionEnd = new java.sql.Timestamp(format.parse("20150608000000").getTime)
    val check_mode = true

    println("cities=" + cities)
    println("predictionStart=" + predictionStart)
    println("predictionEnd=" + predictionEnd)
    println("check_mode=" + check_mode)

    for (i <- cities.indices) {

      val city_name = cities(i).replace(" ", "-")
      val sufix = "-" + formatMonthDay.format(predictionStart) + "-" + formatMonthDay.format(predictionEnd)
      val views_path = base_path + "y_for_prediction/views/5_months/" + city_name + sufix + "/part*"
      val logs_path = base_path + "y_for_prediction/logs/5_months/" + city_name + sufix + "/part*"
      val results_path = base_path + "y_for_prediction/views_on_tuning/5_months/" + city_name + sufix

      println("\n" + "#" * 20 + " " + cities(i) + " " + "#" * 20 + "\n")
      println("views_path=" + views_path)
      println("logs_path=" + logs_path)
      println("results_path=" + results_path)

      val logs = sc.textFile(logs_path)
        .filter(s => !s.isEmpty)
        .map(s => {
          val sp = s.split(",")
          val dt = new java.sql.Timestamp(formatUTC.parse(sp(1)).getTime)
          Log(sp(0), dt, sp(2), sp(3), sp(4), if (sp.length>5) sp(5) else "")
        })
        .groupBy(_.device_id).collect().toMap

      println("Logs map created.")

      val devicesViews = sc.textFile(views_path)
        .filter(s => !s.isEmpty)
        .map(s => {
          val splited = s.split(",").toList
          val device_id = splited.head
          val views = splited.slice(1, s.length)
            .map(v => {
              val sp = v.split("\\|")
              val dt = new java.sql.Timestamp(format.parse(sp(0)).getTime)
              ViewingData(event_date_time = dt, prog_code = sp(1), duration = sp(2).toInt)
            })

          val device_logs = logs(device_id)
          val filtered_views = views.filter(view => {
            // Find close logs
            val closeLogs = device_logs.filter(log => log._type == "T" &&
              TimeUnit.MILLISECONDS.toSeconds(Math.abs(log.event_date.getTime - view.event_date_time.getTime)) < 30)
            closeLogs.nonEmpty
          })

          (device_id, filtered_views)
        })

      if(check_mode){
        val _devicesViews = sc.textFile(views_path)
          .filter(s => !s.isEmpty)
          .map(s => {
            val splited = s.split(",").toList
            val device_id = splited.head
            val views = splited.slice(1, s.length)
              .map(v => {
                val sp = v.split("\\|")
                val dt = new java.sql.Timestamp(format.parse(sp(0)).getTime)
                ViewingData(event_date_time = dt, prog_code = sp(1), duration = sp(2).toInt)
              })
            (device_id, views)
          })
        println("Total regular views " + _devicesViews.flatMap(d => d._2).count())
        println("Total filtered views " + devicesViews.flatMap(d => d._2).count())
      }

      if(!check_mode) {
        if(FileSystem.get(new URI("s3n://magnet-fwm"), sc.hadoopConfiguration).exists(new Path(results_path)))
        {
          println("Deleting " + results_path)
          FileSystem.get(new URI("s3n://magnet-fwm"), sc.hadoopConfiguration).delete(new Path(results_path), true)
        }
        devicesViews
          .map(d => d._1 + "," + d._2.mkString(","))
          .saveAsTextFile(results_path, classOf[GzipCodec])
      }

    }

    //</editor-fold>

    println("done!")
  }
}

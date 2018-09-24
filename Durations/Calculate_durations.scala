package SetTopBoxes.Durations

import java.text.SimpleDateFormat

import org.apache.spark.sql.SparkSession
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

/**
  * Created by yurav on 19/04/2017.
  */
object Calculate_durations {

  val spark: SparkSession = SparkSession
    .builder()
    .master("local")
    .appName("Scala Spark SQL")
    .config("spark.sql.warehouse.dir", "file:///tmp/spark-warehouse")
    .getOrCreate()

  case class ViewingData(
                          device_id_unique: String,
                          log_type: Int, // 0 - view, 1- log
                          event_date_time: java.sql.Timestamp,
                          station_num: String,
                          prog_code: String,
                          event_type: String,
                          event_value: String,
                          event_name: String,
                          event_id: Int,
                          var duration: Int,
                          var prog_duration: Int
                        ) extends Serializable

  /** Main function */
  def main(args: Array[String]): Unit = {
    @transient val conf: SparkConf = new SparkConf().setMaster("local").setAppName("Devices")
    @transient val sc: SparkContext = new SparkContext(conf)

    val cities = List[(String, Int)](("Abilene-Sweetwater", 6), ("Lake Charles", 6), ("Seattle-Tacoma", 8))
    val format = new SimpleDateFormat("yyyyMMddHHmmss")

    val programsDurations = sc.textFile("s3n://magnet-fwm/home/TsviKuflik/rpt_programs/SintecMedia.rpt_programs.date_2015-01*.gz," +
      "s3n://magnet-fwm/home/TsviKuflik/rpt_programs/SintecMedia.rpt_programs.date_2015-02*.gz," +
      "s3n://magnet-fwm/home/TsviKuflik/rpt_programs/SintecMedia.rpt_programs.date_2015-03*.gz," +
      "s3n://magnet-fwm/home/TsviKuflik/rpt_programs/SintecMedia.rpt_programs.date_2015-04*.gz," +
      "s3n://magnet-fwm/home/TsviKuflik/rpt_programs/SintecMedia.rpt_programs.date_2015-05-01*.gz")
      .map(s => {
        val sp = s.split("\\|")
        val prog_id = sp(0)
        (prog_id, sp(5).toDouble.toInt)
      })
      .reduceByKey((a, b) => {
        if (a > b) {
          a
        } else {
          b
        }
      }).collect().toMap

    for (i <- cities.indices) {
      val cityname = cities(i)._1
      val devicesSource = "s3n://magnet-fwm/home/TsviKuflik/devices_views/devices_120_hist_" + cityname + "_ordered/part*"

      val devicesWithViews = sc.textFile(devicesSource)
        .map(x => {
          val firstComma = x.indexOf(",")
          val device_id = x.substring(1, firstComma)
          val viewsStrings = x.substring(firstComma + 6, x.length - 2).split(",")
          val views = viewsStrings.map(s => {
            val sp = s.trim.split("\\|")
            val dt = new java.sql.Timestamp(format.parse(sp(1) + sp(2)).getTime)
            ViewingData("d", sp(0).toInt, dt, sp(3), sp(4), sp(5), sp(6), sp(7), sp(8).toInt, 0, 0)
          }).toList
          (device_id, views)
        }
        )

      //programsDurations.take(30).foreach(p=>{ println(p)})
      //println("progs " + programsDurations.keys.size)

      val viewsDur = devicesWithViews.map { d => {
        var prevView: ViewingData = null
        for (j <- d._2.indices) {
          if (prevView == null && d._2(j).log_type == 0
            && d._2(j).prog_code != "MP_MISSINGPROGRAM" && d._2(j).prog_code != "DVRPGMKEY") {
            prevView = d._2(j)
          }
          // OFF event or next view
          else if (prevView != null &&
            (d._2(j).log_type == 0 || (d._2(j).log_type == 1 && d._2(j).event_name == "OFF"))) {

            val diff = d._2(j).event_date_time.getTime - prevView.event_date_time.getTime
            val totalMinutes = (diff / 60000).toInt
            prevView.duration = totalMinutes
            val progDur = programsDurations.get(prevView.prog_code)
            if (progDur.isDefined) {
              prevView.prog_duration = progDur.get
              if (prevView.prog_duration < prevView.duration) {
                prevView.duration = prevView.prog_duration
              }
            }
            if (d._2(j).log_type == 1) {
              prevView = null
            }
            else {
              prevView = d._2(j)
            }
          }
        }
        d
      }
      }

      val views = viewsDur.map(d => {
        (d._1, d._2.filter(l => l.log_type == 0).map(l => {
          //l.log_type + "|" +
          format.format(l.event_date_time) + "|" + l.station_num + "|" + l.prog_code +
            //"|" + l.event_type+"|" + l.event_value + "|" + l.event_name + "|" + l.event_id +
            "|" + l.duration + "|" + l.prog_duration
        }))
      })

      //      views.take(5).foreach(d=>{
      //        for(j<-0 until 100){
      //          println(d._1,d._2(j))
      //        }
      //      })

      val city_name = cities(i)._1

      views
        .partitionBy(new HashPartitioner(15))
        .saveAsTextFile("s3n://magnet-fwm/home/TsviKuflik/devices_views/devices_120_hist_" + city_name + "_ord_dur")
    }
    print("done!")
  }
}

// results:{
//  "Abilene-Sweetwater":6,
//  "Lake Charles":6,
//  "Seattle-Tacoma":8
// }

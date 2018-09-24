package SetTopBoxes.Prediction3

import java.io.Serializable
import java.net.URI
import java.text.SimpleDateFormat
import java.util.concurrent.TimeUnit
import java.util.Calendar

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.compress.GzipCodec
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

//<editor-fold desc="Case classes">

class ViewingData(val event_date_time: java.sql.Timestamp,
                  val prog_code: String,
                  val duration: Int) extends Serializable {

  override def toString: String = {
    val dt = new java.text.SimpleDateFormat("yyyyMMddHHmmss").format(event_date_time)
    s"$dt|$prog_code|$duration"
  }
}

//</editor-fold>

/**
  * Created by yurav on 19/04/2017.
  */
object CreateViewedForBaseLineProgs {

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

    import spark.implicits._

    //<editor-fold desc="Run area">

    println("\n" + "#" * 70 + " CreateViewedForBaseLineProgs " + "#" * 70 + "\n")

    val cities = List("Amarillo", "Parkersburg", "Little Rock-Pine Bluff", "Seattle-Tacoma")
    val notAllowedPRograms = Array[String]("MP_MISSINGPROGRAM", "DVRPGMKEY"
      , "EP000009937449", "SH000191120000", "SH000000010000", "H002786110000", "SH015815970000", "SH000191160000"
      , "SH000191300000", "SH000191680000", "SH001083440000", "SH003469020000", "SH007919190000", "SH008073210000"
      , "SH014947440000", "SH016938330000", "SH018049520000", "SH020632040000", "SP003015230000")

    val format = new SimpleDateFormat("yyyyMMddHHmmss")
    val dayHourMinutesFormat = new SimpleDateFormat("yyyyMMddHHmm")
    val formatMonthDay = new SimpleDateFormat("MMdd")
    val base_path = "s3n://magnet-fwm/home/TsviKuflik/"
    val views_path = base_path + "y_views_with_durations/*.gz"
    val predictionStart = new java.sql.Timestamp(format.parse("20150601000000").getTime)
    val predictionEnd = new java.sql.Timestamp(format.parse("20150608000000").getTime)
    val time_interval = 5

    for (i <- cities.indices) {

      val city_name = cities(i).replace(" ", "-")
      val devicesSource = base_path + "y_devices/5_months/" + city_name + "/part*"
      val results_path = base_path + "y_for_prediction/views_for_base/progs/" + city_name + "-" +
        formatMonthDay.format(predictionStart) + "-" + formatMonthDay.format(predictionEnd)

      val devicesForThisCity = sc.textFile(devicesSource).map(d => d.trim).collect()
      println("working on " + city_name + " devices in city " + devicesForThisCity.length)
      println("source=" + devicesSource)
      println("dest=" + results_path)
      // devices rdd (device_id,List[ViewingData]) Devices with their views
      val views = spark.read
        .format("com.databricks.spark.csv")
        .load(views_path)
        .filter($"_c0" isin (devicesForThisCity: _*))
        .select(
          $"_c0", // device_id
          $"_c1".cast("timestamp"), // date
          $"_c2", // channel number
          $"_c3", // program code
          $"_c4".cast("int"), // duration
          $"_c5".cast("int") // prog_duration
        )
        .where($"_c3" =!= "MP_MISSINGPROGRAM" && $"_c3" =!= "DVRPGMKEY" && $"_c4" > 5)
        .rdd
        .map(r => {
          val dt = new java.sql.Timestamp(r.getTimestamp(1).getTime)
          new ViewingData(dt, r.getString(3), r.getInt(4))
        })
        .filter(v => v.event_date_time.getTime > predictionStart.getTime && v.event_date_time.getTime < predictionEnd.getTime
          && !notAllowedPRograms.contains(v.prog_code))

      val flated = views.flatMap(v=>{
          var times:ListBuffer[String] = ListBuffer[String]()

          var dt = v.event_date_time
          var dur = v.duration

          val cal = Calendar.getInstance
          cal.setTime(dt)
          val minutes = cal.get(Calendar.MINUTE)

          if((minutes%time_interval)!=0){
            if((minutes%time_interval)<(time_interval/2)){
              dt = new java.sql.Timestamp(dt.getTime - TimeUnit.MINUTES.toMillis(minutes%time_interval))
            }
            else{
              dt = new java.sql.Timestamp(dt.getTime + TimeUnit.MINUTES.toMillis(time_interval - minutes%time_interval))
            }
          }
          while(dur>0){
            times.append(dayHourMinutesFormat.format(dt))
            dt = new java.sql.Timestamp(dt.getTime + TimeUnit.MINUTES.toMillis(time_interval))
            dur-=time_interval
          }
          times.map(time=>(time,(v.prog_code,1)))
        })

      if (FileSystem.get(new URI("s3n://magnet-fwm"), sc.hadoopConfiguration).exists(new Path(results_path))) {
        println("Deleting " + results_path)
        FileSystem.get(new URI("s3n://magnet-fwm"), sc.hadoopConfiguration).delete(new Path(results_path), true)
      }

      flated
        .groupByKey()
        .map(t=>{
          val total = t._2.size
          val counts = t._2.groupBy(v=>v._1)
            .map(v=>{
              val sum = v._2.map(c=>c._2).sum/total.toDouble
              (v._1,sum)
            })
            .toList.sortBy(-_._2)

          t._1 + "|" + counts.mkString("|")
        })
        .saveAsTextFile(results_path, classOf[GzipCodec])
    }


    println("done!")

    //</editor-fold>

  }
}

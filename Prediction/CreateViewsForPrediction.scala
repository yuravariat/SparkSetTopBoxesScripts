package SetTopBoxes.Prediction

import java.io.Serializable
import java.text.SimpleDateFormat
import java.util.concurrent.TimeUnit

import org.apache.hadoop.io.compress.GzipCodec
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

//<editor-fold desc="Case classes">

class ViewingData(val device_id_unique: String,
                  val event_date_time: java.sql.Timestamp,
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
object CreateViewsForPrediction {

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

    println("\n" + "#" * 70 + " CreateViewsForPrediction " + "#" * 70 + "\n")

    val cities = List("Amarillo", "Parkersburg", "Little Rock-Pine Bluff", "Seattle-Tacoma")
    val notAllowedPRograms = Array[String]("MP_MISSINGPROGRAM", "DVRPGMKEY"
      , "EP000009937449", "SH000191120000", "SH000000010000", "H002786110000", "SH015815970000", "SH000191160000"
      , "SH000191300000", "SH000191680000", "SH001083440000", "SH003469020000", "SH007919190000", "SH008073210000"
      , "SH014947440000", "SH016938330000", "SH018049520000", "SH020632040000", "SP003015230000")

    val format = new SimpleDateFormat("yyyyMMddHHmmss")
    val formatMonthDay = new SimpleDateFormat("MMdd")
    val base_path = "s3n://magnet-fwm/home/TsviKuflik/"
    val views_path = base_path + "y_views_with_durations/*.gz"
    val predictionStart = new java.sql.Timestamp(format.parse("20150601000000").getTime)
    val predictionEnd = new java.sql.Timestamp(format.parse("20150615000000").getTime)

    for (i <- cities.indices) {

      val city_name = cities(i).replace(" ","-")
      val devicesSource = base_path + "y_devices/5_months/" + city_name + "/part*"
      val results_path = base_path + "y_for_prediction/views/5_months/" + city_name + "-" +
        formatMonthDay.format(predictionStart) + "-" + formatMonthDay.format(predictionEnd)

      val devicesForThisCity = sc.textFile(devicesSource).map(d => d.trim).collect()
      println("working on " + city_name + " devices in city " + devicesForThisCity.length)
      println("source="+devicesSource)
      println("dest="+results_path)
      // devices rdd (device_id,List[ViewingData]) Devices with their views
      spark.read
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
          (r.getString(0), new ViewingData(r.getString(0), dt, r.getString(3), r.getInt(4)))
        })
        .filter(v => v._2.event_date_time.getTime > predictionStart.getTime && v._2.event_date_time.getTime < predictionEnd.getTime
          && !notAllowedPRograms.contains(v._2.prog_code))
        .groupByKey()
        .mapValues(v => v.toList.sortBy(c => c.event_date_time.getTime))
        .map(d=> d._1 + "," + d._2.mkString(","))
        .saveAsTextFile(results_path, classOf[GzipCodec])
    }
    println("done!")

    //</editor-fold>

  }
}

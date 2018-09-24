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

case class ViewingData(
                        device_id_unique: String,
                        event_date_time: java.sql.Timestamp,
                        prog_code: String,
                        var duration: Int,
                        prog_duration: Int
                      ) extends Serializable {
  override def toString: String =
    s"$device_id_unique|$event_date_time|$prog_code|$duration|$prog_duration"
}

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

    val base_path = "s3n://magnet-fwm/home/TsviKuflik/"
    val timeStampFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")
    val format = new SimpleDateFormat("yyyyMMddHHmmss")
    val start = new java.sql.Timestamp(format.parse("20150601000000").getTime)
    val end = new java.sql.Timestamp(format.parse("20150608000000").getTime)

    val city_name = "Amarillo"
    val device_id = "17719-0000014765ea"

    val devices_views_path = base_path + "y_sequences/step0/" + city_name + "/0101-0615/*.gz"
    val results_path = base_path + "y_sequences/step14/" + city_name + "/" + device_id

    val devicesViews = sc.textFile(devices_views_path)
      .filter(s=>s.startsWith(device_id))
      .map(s => {
        val sp = s.split("\\|")
        val dt = new java.sql.Timestamp(timeStampFormat.parse(sp(1)).getTime)
        ViewingData(sp(0), dt, sp(2), sp(3).toInt, sp(4).toInt)
      })
      .sortBy(-_.event_date_time.getTime)

    if (FileSystem.get(new URI("s3n://magnet-fwm"), sc.hadoopConfiguration).exists(new Path(results_path))) {
      println("Deleting " + results_path)
      FileSystem.get(new URI("s3n://magnet-fwm"), sc.hadoopConfiguration).delete(new Path(results_path), true)
    }

    devicesViews.coalesce(1).saveAsTextFile(results_path)

    println("\n" + "#" * 200 + "\n")
    println("done!")

    //</editor-fold>
  }
}

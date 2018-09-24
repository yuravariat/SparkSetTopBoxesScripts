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

    val chosen_devices_path = base_path + "y_devices/5_months/" + city_name + "/part-00000*"
    val deviceForThisCity = sc.textFile(chosen_devices_path).map(c => c.trim).collect()
    println("city " + city_name + " devices " + deviceForThisCity.length)


    val devices_views_path = base_path + "y_sequences/step0/" + city_name + "/0101-0615/*.gz"
    val results_path = base_path + "y_sequences/step12/" + city_name

    val devicesViews = sc.textFile(devices_views_path)
      .map(s => {
        val sp = s.split("\\|")
        val dt = new java.sql.Timestamp(timeStampFormat.parse(sp(1)).getTime)
        if (dt.getTime > start.getTime) {
          if (dt.getTime > end.getTime) {
            (sp(0), (0, 0))
          }
          else {
            (sp(0), (0, 1))
          }
        }
        else {
          (sp(0), (1, 0))
        }
      })
      .reduceByKey((t_1, t_2) => {
        (t_1._1 + t_2._1, t_1._2 + t_2._2)
      })

    val prediction_results_path = base_path + "y_predictions/5_months_gen/Amarillo_raw_1/*.gz"
    val predictionDevices = sc.textFile(prediction_results_path)
      .map(s => {
        val sp = s.split("\\|")
        val precisionAt1 = sp(4).toInt
        val prec = if (precisionAt1==0) 1 else 0
        (sp(0), (1,prec))
      }
      )
      .reduceByKey((t_1, t_2) => {
        (t_1._1 + t_2._1, t_1._2 + t_2._2)
      })

    val joined = devicesViews.join(predictionDevices)
      .sortBy(-_._2._1._2)
      .map(d=>{
        val deviceid = d._1
        val learned_views = d._2._1._1
        val predict_views = d._2._1._2
        val pred_attempts = d._2._2._1
        val precision = d._2._2._2
        deviceid + "," + learned_views + "," + predict_views + "," + pred_attempts + "," + precision + "," + precision/pred_attempts.toDouble
      })

    joined.collect().foreach(d=> {
      println(d)
    })

    if (FileSystem.get(new URI("s3n://magnet-fwm"), sc.hadoopConfiguration).exists(new Path(results_path))) {
      println("Deleting " + results_path)
      FileSystem.get(new URI("s3n://magnet-fwm"), sc.hadoopConfiguration).delete(new Path(results_path), true)
    }

    joined.coalesce(1).saveAsTextFile(results_path)

    println("\n" + "#" * 200 + "\n")
    println("done!")

    //</editor-fold>
  }
}

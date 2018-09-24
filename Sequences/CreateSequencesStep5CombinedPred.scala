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
object CreateSequencesStep5CombinedPred {

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

    /*
    Script that joins clustering results with prediction rates in order to see the correlation
      between the stability and prediction rates.
    */

    val base_path = "s3n://magnet-fwm/home/TsviKuflik/"
    val timeStampFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")
    val formatMonthDay = new SimpleDateFormat("MMdd")
    val format = new SimpleDateFormat("yyyyMMddHHmmss")
    val start = new java.sql.Timestamp(format.parse("20150101000000").getTime)
    val end = new java.sql.Timestamp(format.parse("20150615000000").getTime)

    val city_name = "Amarillo"
    val chosenPredictSource = "5_months_gen"
    val chosenParts = 10
    val chosenKNumber = 35
    val rate_from_views_only = true

    val chosen_devices_path = base_path + "y_devices/5_months/" + city_name + "/part-00000*"
    val deviceForThisCity = sc.textFile(chosen_devices_path).map(c => c.trim).collect()
    println("city " + city_name + " devices " + deviceForThisCity.length)

    println("start=" + start)
    println("end=" + end)
    println("chosen_devices_path=" + chosen_devices_path)
    println("city_name=" + city_name)
    println("chosenPredictSource=" + chosenPredictSource)
    println("chosenParts=" + chosenParts)
    println("chosenKNumber=" + chosenKNumber)
    println("rate_from_views_only=" + rate_from_views_only)

    val devices_views_path = base_path + "y_sequences/step0/" + city_name + "/" +
      formatMonthDay.format(start) + "-" + formatMonthDay.format(end) + "/*.gz"
    val results_base_path = base_path + "y_sequences/step5_combined_pred/" + city_name + "/" +
      (if (rate_from_views_only) "from_views_" else "") +
      formatMonthDay.format(start) + "-" + formatMonthDay.format(end) + "/k_" + chosenKNumber
    val results_path = results_base_path + "/points"
    val results_cntr_path = results_base_path + "/centroids"
    val clustered_data_path = base_path + "y_sequences/step4_kmeans/" + city_name + "/" +
      (if (rate_from_views_only) "from_views_" else "") +
      formatMonthDay.format(start) + "-" + formatMonthDay.format(end) + "/k_" + chosenKNumber + "/clustered/*.gz"

    println("devices_views_path=" + devices_views_path)
    println("clustered_data_path=" + clustered_data_path)
    println("results_path=" + results_path)

    //<editor-fold desc="Create prediction per device RDD">

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

    val prediction_results_path = base_path + "y_predictions/" + chosenPredictSource + "/" + city_name + "_raw_" + chosenParts + "/*.gz"
    val predictionDevices = sc.textFile(prediction_results_path)
      .map(s => {
        val sp = s.split("\\|")
        val precisionAt1 = sp(4).toInt
        val prec = if (precisionAt1 == 0) 1 else 0
        (sp(0), (1, prec))
      }
      )
      .reduceByKey((t_1, t_2) => {
        (t_1._1 + t_2._1, t_1._2 + t_2._2)
      })

    val final_preict_per_device = devicesViews.join(predictionDevices)
      .sortBy(-_._2._1._2)
      .map(d => {
        val device_id = d._1
        val learned_views = d._2._1._1
        val predict_views = d._2._1._2
        val pred_attempts = d._2._2._1
        val precision = d._2._2._2

        (device_id, (learned_views, predict_views, pred_attempts, precision, precision / pred_attempts.toDouble))
      })

    //</editor-fold>

    //<editor-fold desc="Load clustered data">

    val clustered_data = sc.textFile(clustered_data_path)
      .map(s => {
        val sp = s.split(",")
        // (device_id, cluster_num, vector)
        (sp(1), (sp(0).toInt, sp.drop(2).map(_.toDouble)))
      })

    //</editor-fold>

    //<editor-fold desc="Join them">

    val joined = clustered_data
      .join(final_preict_per_device)

    //</editor-fold>

    //<editor-fold desc="Centroids and averages">

    val centroids = joined
      .map(r => {
        // (clust_num,vector,predict...)
        (r._2._1._1, (r._2._1._2, r._2._2._1, r._2._2._2, r._2._2._3, r._2._2._4, r._2._2._5))
      })
      .groupByKey()
      .mapValues(v => {
        val length = v.head._1.length + 5
        val count = v.size
        val sumArray = new Array[Double](length)

        for (item <- v) {
          for (i <- item._1.indices) {
            sumArray(i) += item._1(i)
          }
          sumArray(length - 5) += item._2
          sumArray(length - 4) += item._3
          sumArray(length - 3) += item._4
          sumArray(length - 2) += item._5
          sumArray(length - 1) += item._6
        }
        for (i <- sumArray.indices) {
          sumArray(i) = sumArray(i)/count.toDouble
        }

        sumArray
      })

    //</editor-fold>

    if (FileSystem.get(new URI("s3n://magnet-fwm"), sc.hadoopConfiguration).exists(new Path(results_path))) {
      println("Deleting " + results_path)
      FileSystem.get(new URI("s3n://magnet-fwm"), sc.hadoopConfiguration).delete(new Path(results_path), true)
    }

    joined
      .sortBy(_._2._1._1)
      .map(r => {
        s"${r._1},${r._2._1._1},${r._2._1._2.mkString(",")},${r._2._2._1},${r._2._2._2},${r._2._2._3},${r._2._2._4},${r._2._2._5}"
      })
      .coalesce(1)
      .saveAsTextFile(results_path)

    if (FileSystem.get(new URI("s3n://magnet-fwm"), sc.hadoopConfiguration).exists(new Path(results_cntr_path))) {
      println("Deleting " + results_cntr_path)
      FileSystem.get(new URI("s3n://magnet-fwm"), sc.hadoopConfiguration).delete(new Path(results_cntr_path), true)
    }

    centroids
      .sortBy(_._1)
      .map(r => {
        s"${r._1},${r._2.mkString(",")}"
      })
      .coalesce(1)
      .saveAsTextFile(results_cntr_path)

    println("\n" + "#" * 200 + "\n")
    println("done!")

    //</editor-fold>
  }
}
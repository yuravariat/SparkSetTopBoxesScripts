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

/**
  * Created by yurav on 19/04/2017.
  */
object CreateSequencesStep3Matrices {

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

    //<editor-fold desc="Run area">

    /*
    Script that creates matrix where columns are time slots rows are days.
     In each cell appears stability rate. Which means how the dominant genres is stable what percentage it take in this time slot.
    result example:
    17719-000000be6d1d,0.21212121212121213,0.21212121212121213,0.22424242424242424,0.2303030303030
    17719-000000ad5152,0.42424242424242425,0.3878787878787879,0.38181818181818183,0.38181818181818
    17719-000002b086b8,0.08484848484848485,0.08484848484848485,0.08484848484848485,0.0909090909090
    17719-000000a01d74,0.14545454545454545,0.07272727272727272,0.04242424242424243,0.0424242424242
    17719-000000a459ee,0.06060606060606061,0.05454545454545454,0.05454545454545454,0.0545454545454
    17719-000002bbef04,0.2545454545454545,0.23636363636363636,0.23636363636363636,0.21818181818181
    */

    println("\n" + "#" * 70 + " Sequences Matrices " + "#" * 70 + "\n")

    val base_path = "s3n://magnet-fwm/home/TsviKuflik/"

    val format = new SimpleDateFormat("yyyyMMddHHmmss")
    val formatMonthDay = new SimpleDateFormat("MMdd")

    val start = new java.sql.Timestamp(format.parse("20150101000000").getTime)
    val end = new java.sql.Timestamp(format.parse("20150615000000").getTime)
    val rate_from_views_only = true
    val cities = List[String]("Amarillo", "Parkersburg", "Little Rock-Pine Bluff", "Seattle-Tacoma")
    val slotMinutes = 15

    println("rate_from_views_only=" + rate_from_views_only)

    for (city <- cities) {

      val city_name = city.replace(" ", "-")
      val path = base_path + "y_sequences/step2/" + city_name + "/" +
        formatMonthDay.format(start) + "-" + formatMonthDay.format(end) + "/*.gz"

      println("city=" + city)
      println("path=" + path)
      val results_path = base_path + "y_sequences/step3_matrices/" + city_name + "/" +
        (if (rate_from_views_only) "from_views_" else "") +
        formatMonthDay.format(start) + "-" + formatMonthDay.format(end)

      println("results_path=" + results_path)
      println("\n" + "#" * 200 + "\n")

      val matrices = sc.textFile(path)
        .map(_.split(","))
        .groupBy(_.head)
        .map(g => {
          val days_vectors: Array[Array[String]] = g._2.toArray

          val total_days_rows = days_vectors.length
          val vectors_length_cols = days_vectors.head.length - 2

          val frequenciesMatrix = new Array[Double](vectors_length_cols)

          for (col <- 0 until vectors_length_cols) {
            val list = ListBuffer[String]()
            val col_index = col + 2

            for (row <- 0 until total_days_rows) {
              if (days_vectors(row)(col_index) != "-" && days_vectors(row)(col_index) != "nogenre") {
                list.append(days_vectors(row)(col_index))
              }
            }

            if (list.nonEmpty) {
              val mostFreqGenre = list.groupBy(identity).mapValues(_.size).maxBy(_._2)
              if(rate_from_views_only){
                frequenciesMatrix(col) = mostFreqGenre._2 / list.length.toDouble
              }
              else {
                frequenciesMatrix(col) = mostFreqGenre._2 / total_days_rows.toDouble
              }
            }
            else {
              frequenciesMatrix(col) = 0.0
            }
          }

          (g._1, frequenciesMatrix)
        })

      if (FileSystem.get(new URI("s3n://magnet-fwm"), sc.hadoopConfiguration).exists(new Path(results_path))) {
        println("Deleting " + results_path)
        FileSystem.get(new URI("s3n://magnet-fwm"), sc.hadoopConfiguration).delete(new Path(results_path), true)
      }

      matrices
        .map(d => d._1 + "," +  d._2.mkString(","))
        .coalesce(1)
        .saveAsTextFile(results_path)
    }
    println("\n" + "#" * 200 + "\n")
    println("done!")

    //</editor-fold>
  }
}

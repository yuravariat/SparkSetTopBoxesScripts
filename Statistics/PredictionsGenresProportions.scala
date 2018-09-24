package SetTopBoxes.Statistics

import java.io.Serializable
import java.net.URI

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

object PredictionsGenresTopGenres {
  @transient val conf: SparkConf = new SparkConf().setMaster("local").setAppName("Devices")
  @transient val sc: SparkContext = new SparkContext(conf)
  val spark: SparkSession = SparkSession
    .builder()
    .master("local")
    .appName("Scala Spark SQL")
    .config("spark.sql.warehouse.dir", "file:///tmp/spark-warehouse")
    .getOrCreate()

  import spark.implicits._

  def main(args: Array[String]): Unit = {

    //<editor-fold desc="Run area">

    /*
    Script that calulates what percent of each genre was predicted successfuly
    */

    println("\n" + "#" * 70 + " Predictions Genres Proportions " + "#" * 70 + "\n")

    // Map cityname => (timediff, k number)
    val cities = List("Amarillo", "Parkersburg", "Little Rock-Pine Bluff", "Seattle-Tacoma")
    val basePath = "s3n://magnet-fwm/home/TsviKuflik/"
    val partsOfDay = 10
    val useDataWithTimeDecay = true
    val predictOnlyOnTuningEvent = false
    val timeDecayRate = "_70"
    val div5new = false
    val precisionAt = 0
    val isGroup = false

    println("cities=" + cities.mkString(", "))
    println("timeDecayRate=" + timeDecayRate)
    println("div5new=" + div5new)
    println("partsOfDay=" + partsOfDay)
    println("useDataWithTimeDecay=" + useDataWithTimeDecay)
    println("timeDecayRate=" + timeDecayRate)
    println("predictOnlyOnTuningEvent=" + predictOnlyOnTuningEvent)
    println("basePath=" + basePath)
    println("precisionAt=" + precisionAt)
    println("isGroup=" + isGroup)
    println("\n")


    val GenresArray = sc.textFile(basePath + "y_programs/genres/part*")
      .map(_.split("\\|"))
      .filter(s => !s(0).isEmpty)
      .map(s => s(0))
      .collect()
      .sortBy(s => s)
      .toList

    for (city_name <- cities) {

      val city_name_safe = city_name.replace(" ", "-")

      println("\n" + "#" * 50 + city_name + "#" * 50 + "\n")

      val results_path = basePath + "y_predictions/5_months_gen" +
        (if (useDataWithTimeDecay) "_with_decay" + timeDecayRate else "") +
        (if (predictOnlyOnTuningEvent) "_on_tuning" else "") +
        "/" + city_name_safe + "_raw_" + partsOfDay + (if (div5new) "_v2" else "") + "/part*"

      val destination_path = basePath + "y_predictions/5_months_gen" +
        (if (useDataWithTimeDecay) "_with_decay" + timeDecayRate else "") +
        (if (predictOnlyOnTuningEvent) "_on_tuning" else "") +
        "/genres_proportions_" + partsOfDay + (if (div5new) "_v2" else "") + "/" + city_name_safe +
        "_pr_" + (precisionAt + 1)

      println("results_base_path=" + results_path)
      println("destination_base_path=" + destination_path)
      println("\n")

      val precision_by_genres = sc.textFile(results_path)
        .flatMap(s => {
          val sp = s.split("\\|")
          //val city = sp(1)
          val genres = ListBuffer[(Int,(Int,Int))]()
          val _precisionAt = if (isGroup) sp(5).toInt else sp(4).toInt
          val actualGenres = sp(12).split(",")
          val suggested_genres_ind = if (isGroup) sp(13).split(",").take(precisionAt + 1) else sp(14).split(",").take(precisionAt + 1)
          if (_precisionAt <= precisionAt) {
            for (genInx <- suggested_genres_ind) {
              if(actualGenres.contains(genInx)){
                genres.append((genInx.toInt, (1, 1)))
              }
            }
          }
          else{
            for (genInx <- suggested_genres_ind) {
              genres.append((genInx.toInt, (0, 1)))
            }
          }
          genres
        })
        .reduceByKey((a, b) => {
          (a._1 + b._1, a._2 + b._2)
        })
        .collect()
        .sortBy(_._1)

      val precision_by_genres_names = precision_by_genres
        .map(p => {
          p._1 + "," + GenresArray(p._1) + "," + p._2._1 + "," + p._2._2
        })

      if (FileSystem.get(new URI("s3n://magnet-fwm"), sc.hadoopConfiguration).exists(new Path(destination_path))) {
        println("Deleting " + destination_path)
        FileSystem.get(new URI("s3n://magnet-fwm"), sc.hadoopConfiguration).delete(new Path(destination_path), true)
      }

      sc.parallelize(precision_by_genres_names)
        .coalesce(1)
        .saveAsTextFile(destination_path)
    }

    println("Done!")

    //</editor-fold>
  }
}

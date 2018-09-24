package SetTopBoxes.Prediction

import java.io.Serializable
import java.net.URI
import java.text.SimpleDateFormat
import java.util.concurrent.TimeUnit

import SetTopBoxes.Prediction
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.compress.GzipCodec
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.{Encoder, Encoders, SparkSession, TypedColumn}
import org.apache.spark.sql.functions._
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import org.apache.spark.sql.expressions.Aggregator

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object PredictionResultsStatRawNew {
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

    println("\n" + "#" * 70 + " ProgPredictResultsShow Raw " + "#" * 70 + "\n")

    // Map cityname => (timediff, k number)
    val cities = List("Amarillo", "Parkersburg", "Little Rock-Pine Bluff", "Seattle-Tacoma")
    val partsOfDayList = List(1,10)
    val useDataWithTimeDecayList = List(false) //List(true, false)
    val predictOnlyOnTuningEventList = List(false) //List(true, false)
    val timeDecayRate = "_70"
    val isGenres = true
    val useScaleOf10Genres = true
    val div5new = false
    val isBaseLine = false
    val basePath = "s3n://magnet-fwm/home/TsviKuflik/"

    println("cities=" + cities.mkString(", "))
    println("partsOfDayList=" + partsOfDayList.mkString(", "))
    println("useDataWithTimeDecayList=" + useDataWithTimeDecayList.mkString(", "))
    println("predictOnlyOnTuningEventList=" + predictOnlyOnTuningEventList.mkString(", "))
    println("timeDecayRate=" + timeDecayRate)
    println("div5new=" + div5new)
    println("isBaseLine=" + isBaseLine)
    println("isGenres=" + isGenres)
    println("useScaleOf10Genres=" + useScaleOf10Genres)

    for (partsOfDay <- partsOfDayList) {
      for (useDataWithTimeDecay <- useDataWithTimeDecayList) {
        for (predictOnlyOnTuningEvent <- predictOnlyOnTuningEventList){

          if(predictOnlyOnTuningEvent && !useDataWithTimeDecay){
            // do nothing, not exist
          }
          else{
            //<editor-fold desc="work">
            println("\n" + "#" * 50 + " iteration " + "#" * 50 + "\n")

            var results_base_path = ""
            var destination_base_path = ""

            if(isBaseLine){

              results_base_path = basePath + "y_predictions/5_months/base_line/" +
                (if (isGenres) "genres" else "progs") + "/"

              destination_base_path = basePath + "y_predictions/5_months/base_line/" +
                (if (isGenres) "genres" else "progs") + "/aggregation"
            }
            else{
              results_base_path = basePath + "y_predictions/5_months" + (if (isGenres) "_gen" else "") +
                (if (useDataWithTimeDecay) "_with_decay" + timeDecayRate else "") +
                (if (predictOnlyOnTuningEvent) "_on_tuning" else "") +
                (if (useScaleOf10Genres) "_scale10" else "") + "/"

              destination_base_path = basePath + "y_predictions/5_months" + (if (isGenres) "_gen" else "") +
                (if (useDataWithTimeDecay) "_with_decay" + timeDecayRate else "") +
                (if (predictOnlyOnTuningEvent) "_on_tuning" else "") +
                (if (useScaleOf10Genres) "_scale10" else "") +
                "/raw_" + partsOfDay + (if (div5new) "_v2" else "") + "/"
            }

            println("partsOfDay=" + partsOfDay)
            println("useDataWithTimeDecay=" + useDataWithTimeDecay)
            println("timeDecayRate=" + timeDecayRate)
            println("predictOnlyOnTuningEvent=" + predictOnlyOnTuningEvent)
            println("basePath=" + basePath)
            println("results_base_path=" + results_base_path)
            println("destination_base_path=" + destination_base_path)
            println("\n")

            var results_paths = ListBuffer[String]()
            for (city_name <- cities) {
              val city_name_safe = city_name.replace(" ", "-")
              var path = ""
              if(isBaseLine){
                path = results_base_path + city_name_safe + "/part*"
              }
              else{
                path = results_base_path + city_name_safe + "_raw_" + partsOfDay + (if (div5new) "_v2" else "") + "/part*"
              }
              println(city_name + " path=> " + path)
              results_paths.append(path)
            }
            println("\n")

            var prAt5=5
            var prAt10=10
            var prAt15=15
            var prAt20=20
            if(useScaleOf10Genres){
              prAt5=2
              prAt10=3
              prAt15=4
              prAt20=5
            }

            val results = spark.read
              .option("delimiter", "|")
              .csv(results_paths: _*)
              .select(
                $"_c0" as "device_id",
                $"_c1" as "city",
                $"_c2" cast "int" as "found",
                $"_c3" cast "int" as "gr_found",
                $"_c4" cast "int" as "program_position",
                $"_c5" cast "int" as "gr_program_position",
                $"_c6" cast "int" as "cluster_num",
                $"_c7" cast "int" as "total_cls_num",
                $"_c8" cast "int" as "part",
                $"_c9" cast "TimeStamp" as "event_date_time",
                $"_c10" as "prog_code",
                $"_c11" cast "int" as "translated_progs_num"
              )
              .withColumn("citypart", concat_ws("-", $"city", $"part"))
              .withColumn("gr_pr1", when($"gr_program_position" === 0, 1).otherwise(0))
              .withColumn("gr_pr5", when($"gr_program_position" >= 0 && $"gr_program_position" <= prAt5, 1).otherwise(0))
              .withColumn("gr_pr10", when($"gr_program_position" >= 0 && $"gr_program_position" <= prAt10, 1).otherwise(0))
              .withColumn("gr_pr15", when($"gr_program_position" >= 0 && $"gr_program_position" <= prAt15, 1).otherwise(0))
              .withColumn("gr_pr20", when($"gr_program_position" >= 0 && $"gr_program_position" <= prAt20, 1).otherwise(0))
              .withColumn("pr1", when($"program_position" === 0, 1).otherwise(0))
              .withColumn("pr5", when($"program_position" >= 0 && $"program_position" <= prAt5, 1).otherwise(0))
              .withColumn("pr10", when($"program_position" >= 0 && $"program_position" <= prAt10, 1).otherwise(0))
              .withColumn("pr15", when($"program_position" >= 0 && $"program_position" <= prAt15, 1).otherwise(0))
              .withColumn("pr20", when($"program_position" >= 0 && $"program_position" <= prAt20, 1).otherwise(0))

            //results.show()

            val countColumnName = if (isGenres) "genres" else "progs"

            if(!isBaseLine) {
              val byPartsOfDays = results
                .groupBy($"citypart")
                .agg(
                  first($"city") as "city",
                  first($"part") as "part",
                  first($"total_cls_num") as "total_cls_num",
                  avg($"translated_progs_num") as countColumnName,
                  avg($"gr_pr1") as "gr_pr1",
                  avg($"gr_pr5") as "gr_pr5",
                  avg($"gr_pr10") as "gr_pr10",
                  avg($"gr_pr15") as "gr_pr15",
                  avg($"gr_pr20") as "gr_pr20",
                  avg($"pr1") as "pr1",
                  avg($"pr5") as "pr5",
                  avg($"pr10") as "pr10",
                  avg($"pr15") as "pr15",
                  avg($"pr20") as "pr20",
                  stddev($"gr_pr1") as "stdd_gr_pr1",
                  stddev($"gr_pr5") as "stdd_gr_pr5",
                  stddev($"gr_pr10") as "stdd_gr_pr10",
                  stddev($"gr_pr15") as "stdd_gr_pr15",
                  stddev($"gr_pr20") as "stdd_gr_pr20",
                  stddev($"pr1") as "stdd_pr1",
                  stddev($"pr5") as "stdd_pr5",
                  stddev($"pr10") as "stdd_pr10",
                  stddev($"pr15") as "stdd_pr15",
                  stddev($"pr20") as "stdd_pr20",
                  count($"gr_pr1") as "attempts"
                )
                .sort($"city", $"part")
                .toDF()
                .cache()

              byPartsOfDays.show(50)

              val by_parts_save_to = destination_base_path + "by_parts"
              if (FileSystem.get(new URI("s3n://magnet-fwm"), sc.hadoopConfiguration).exists(new Path(by_parts_save_to))) {
                println("Deleting " + by_parts_save_to)
                FileSystem.get(new URI("s3n://magnet-fwm"), sc.hadoopConfiguration).delete(new Path(by_parts_save_to), true)
              }

              byPartsOfDays
                .coalesce(1)
                .write
                .option("header", "true")
                .csv(by_parts_save_to)
            }

            val byCities = results
              .groupBy($"city")
              .agg(
                first($"total_cls_num") as "total_cls_num",
                avg($"translated_progs_num") as countColumnName,
                avg($"gr_pr1") as "gr_pr1",
                avg($"gr_pr5") as "gr_pr5",
                avg($"gr_pr10") as "gr_pr10",
                avg($"gr_pr15") as "gr_pr15",
                avg($"gr_pr20") as "gr_pr20",
                avg($"pr1") as "pr1",
                avg($"pr5") as "pr5",
                avg($"pr10") as "pr10",
                avg($"pr15") as "pr15",
                avg($"pr20") as "pr20",
                stddev($"gr_pr1") as "stdd_gr_pr1",
                stddev($"gr_pr5") as "stdd_gr_pr5",
                stddev($"gr_pr10") as "stdd_gr_pr10",
                stddev($"gr_pr15") as "stdd_gr_pr15",
                stddev($"gr_pr20") as "stdd_gr_pr20",
                stddev($"pr1") as "stdd_pr1",
                stddev($"pr5") as "stdd_pr5",
                stddev($"pr10") as "stdd_pr10",
                stddev($"pr15") as "stdd_pr15",
                stddev($"pr20") as "stdd_pr20",
                count($"gr_pr1") as "attempts"
              )
              .sort($"city")

            byCities.show()

            val by_cities_save_to = destination_base_path + (if (isBaseLine) "" else "by_cities")
            if (FileSystem.get(new URI("s3n://magnet-fwm"), sc.hadoopConfiguration).exists(new Path(by_cities_save_to))) {
              println("Deleting " + by_cities_save_to)
              FileSystem.get(new URI("s3n://magnet-fwm"), sc.hadoopConfiguration).delete(new Path(by_cities_save_to), true)
            }
            byCities
              .coalesce(1)
              .write
              .option("header", "true")
              .csv(by_cities_save_to)

            //</editor-fold>
          }
        }
      }
    }

    println("Done!")

    //</editor-fold>
  }
}

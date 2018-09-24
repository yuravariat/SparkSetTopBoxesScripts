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

    println("\n" + "#" * 70 + " Predictions Genres Top Genres " + "#" * 70 + "\n")

    // Map cityname => (timediff, k number)
    val cities = List("Amarillo", "Parkersburg", "Little Rock-Pine Bluff", "Seattle-Tacoma")
    val basePath = "s3n://magnet-fwm/home/TsviKuflik/"
    val partsOfDayList = List(10)
    val useDataWithTimeDecayList = List(true) //List(true, false)
    val predictOnlyOnTuningEventList = List(false) //List(true, false)
    val timeDecayRate = "_70"
    val div5new = false

    println("cities=" + cities.mkString(", "))
    println("partsOfDayList=" + partsOfDayList.mkString(", "))
    println("useDataWithTimeDecayList=" + useDataWithTimeDecayList.mkString(", "))
    println("predictOnlyOnTuningEventList=" + predictOnlyOnTuningEventList.mkString(", "))
    println("timeDecayRate=" + timeDecayRate)
    println("div5new=" + div5new)

    val GenresArray = sc.textFile(basePath + "y_programs/genres/part*")
      .map(_.split("\\|"))
      .filter(s => !s(0).isEmpty)
      .map(s => s(0))
      .collect()
      .sortBy(s => s)
      .toList

    def GenreIndexToName = udf((inx: Int ) => {
      GenresArray(inx)
    })
    def StringToIntSeq = udf((list:String ) => {
      list.split(",").map(s=>s.toInt)
    })

    for (partsOfDay <- partsOfDayList) {

      for (useDataWithTimeDecay <- useDataWithTimeDecayList) {
        for (predictOnlyOnTuningEvent <- predictOnlyOnTuningEventList) {

          if (predictOnlyOnTuningEvent && !useDataWithTimeDecay) {
            // do nothing, not exist
          }
          else {
            //<editor-fold desc="work">
            println("\n" + "#" * 50 + " iteration " + "#" * 50 + "\n")

            val results_base_path = basePath + "y_predictions/5_months_gen" +
              (if (useDataWithTimeDecay) "_with_decay" + timeDecayRate else "") +
              (if (predictOnlyOnTuningEvent) "_on_tuning" else "")

            val destination_base_path = basePath + "y_predictions/5_months_gen" +
              (if (useDataWithTimeDecay) "_with_decay" + timeDecayRate else "") +
              (if (predictOnlyOnTuningEvent) "_on_tuning" else "") +
              "/top_genres_" + partsOfDay + (if (div5new) "_v2" else "")

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
              val path = results_base_path + "/" + city_name_safe + "_raw_" + partsOfDay + (if (div5new) "_v2" else "") + "/part*"
              println(city_name + " path=> " + path)
              results_paths.append(path)
            }
            println("\n")

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
                $"_c12" as "prog_genres"
              )
              .withColumn("gr_pr1", when($"gr_program_position" === 0, 1).otherwise(0))
              .withColumn("gr_pr5", when($"gr_program_position" >= 0 && $"gr_program_position" <= 5, 1).otherwise(0))
              .withColumn("gr_pr10", when($"gr_program_position" >= 0 && $"gr_program_position" <= 10, 1).otherwise(0))
              .withColumn("gr_pr15", when($"gr_program_position" >= 0 && $"gr_program_position" <= 15, 1).otherwise(0))
              .withColumn("gr_pr20", when($"gr_program_position" >= 0 && $"gr_program_position" <= 20, 1).otherwise(0))
              .withColumn("pr1", when($"program_position" === 0, 1).otherwise(0))
              .withColumn("pr5", when($"program_position" >= 0 && $"program_position" <= 5, 1).otherwise(0))
              .withColumn("pr10", when($"program_position" >= 0 && $"program_position" <= 10, 1).otherwise(0))
              .withColumn("pr15", when($"program_position" >= 0 && $"program_position" <= 15, 1).otherwise(0))
              .withColumn("pr20", when($"program_position" >= 0 && $"program_position" <= 20, 1).otherwise(0))
              .withColumn("prog_genres_list", StringToIntSeq($"prog_genres"))
              .withColumn("prog_genre", explode($"prog_genres_list"))

            //results.show()

            val groupedDF = results
              .groupBy($"prog_genre")
              .agg(
                sum($"gr_pr1") as "gr_pr1",
                sum($"gr_pr5") as "gr_pr5",
                sum($"gr_pr10") as "gr_pr10",
                sum($"gr_pr15") as "gr_pr15",
                sum($"gr_pr20") as "gr_pr20",
                sum($"pr1") as "pr1",
                sum($"pr5") as "pr5",
                sum($"pr10") as "pr10",
                sum($"pr15") as "pr15",
                sum($"pr20") as "pr20"
              )
              .withColumn("genre_name", GenreIndexToName($"prog_genre"))
              .sort(-$"pr1")

            if (FileSystem.get(new URI("s3n://magnet-fwm"), sc.hadoopConfiguration).exists(new Path(destination_base_path))) {
              println("Deleting " + destination_base_path)
              FileSystem.get(new URI("s3n://magnet-fwm"), sc.hadoopConfiguration).delete(new Path(destination_base_path), true)
            }

            groupedDF
              .coalesce(1)
              .write
              .option("header", "true")
              .csv(destination_base_path)

            //</editor-fold>
          }
        }
      }
    }

    println("Done!")

    //</editor-fold>
  }
}

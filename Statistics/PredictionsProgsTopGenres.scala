package SetTopBoxes.Statistics

import java.io.Serializable
import java.net.URI

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

//<editor-fold desc="Case classes">

case class Prog(
                 index: Long,
                 name: String,
                 genres: String,
                 codes: Seq[String]
               ) extends Serializable

//</editor-fold>

object PredictionsProgsTopGenres {
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

    println("\n" + "#" * 70 + " Predictions top genres " + "#" * 70 + "\n")

    // Map cityname => (timediff, k number)
    val cities = List("Amarillo", "Parkersburg", "Little Rock-Pine Bluff", "Seattle-Tacoma")
    val basePath = "s3n://magnet-fwm/home/TsviKuflik/"
    val programs_path = basePath + "y_programs/programs_genres_groups/part-00000"
    val partsOfDayList = List(10)
    val useDataWithTimeDecayList = List(true) //List(true, false)
    val predictOnlyOnTuningEventList = List(false) //List(true, false)
    val timeDecayRate = "_70"
    val isGenres = false
    val div5new = true

    println("cities=" + cities.mkString(", "))
    println("partsOfDayList=" + partsOfDayList.mkString(", "))
    println("programs_path=" + programs_path)
    println("useDataWithTimeDecayList=" + useDataWithTimeDecayList.mkString(", "))
    println("predictOnlyOnTuningEventList=" + predictOnlyOnTuningEventList.mkString(", "))
    println("timeDecayRate=" + timeDecayRate)
    println("div5new=" + div5new)

    for (partsOfDay <- partsOfDayList) {

      for (useDataWithTimeDecay <- useDataWithTimeDecayList) {
        for (predictOnlyOnTuningEvent <- predictOnlyOnTuningEventList) {

          if (predictOnlyOnTuningEvent && !useDataWithTimeDecay) {
            // do nothing, not exist
          }
          else {
            //<editor-fold desc="work">
            println("\n" + "#" * 50 + " iteration " + "#" * 50 + "\n")
            val results_base_path = basePath + "y_predictions/5_months" +
              (if (useDataWithTimeDecay) "_with_decay" + timeDecayRate else "") +
              (if (isGenres) "_gen" else "") + (if (predictOnlyOnTuningEvent) "_on_tuning" else "")

            val destination_base_path = basePath + "y_predictions/5_months" +
              (if (useDataWithTimeDecay) "_with_decay" + timeDecayRate else "") +
              (if (isGenres) "_gen" else "") + (if (predictOnlyOnTuningEvent) "_on_tuning" else "") +
              "/raw_top_genres_" + partsOfDay + (if (div5new) "_v2" else "")

            println("partsOfDay=" + partsOfDay)
            println("isGenres=" + isGenres)
            println("useDataWithTimeDecay=" + useDataWithTimeDecay)
            println("timeDecayRate=" + timeDecayRate)
            println("predictOnlyOnTuningEvent=" + predictOnlyOnTuningEvent)
            println("basePath=" + basePath)
            println("results_base_path=" + results_base_path)
            println("destination_base_path=" + destination_base_path)
            println("\n")

            var results_path = basePath + "y_predictions/5_months" +
              (if (useDataWithTimeDecay) "_with_decay" + timeDecayRate else "") +
              (if (isGenres) "_gen" else "") + (if (predictOnlyOnTuningEvent) "_on_tuning" else "") +
              "/raw_top_progs_" + partsOfDay + (if (div5new) "_v2" else "") + "/part*"

            println("results_path=" + results_path)
            println("\n")

            val results = spark.read
              //.option("delimiter", "|")
              .option("header", "true")
              .csv(results_path)
              .select(
                $"prog_code",
                $"gr_pr1" cast "int" as "gr_pr1",
                $"gr_pr5" cast "int" as "gr_pr5",
                $"gr_pr10" cast "int" as "gr_pr10",
                $"gr_pr15" cast "int" as "gr_pr15",
                $"gr_pr20" cast "int" as "gr_pr20",
                $"pr1" cast "int" as "pr1",
                $"pr5" cast "int" as "pr5",
                $"pr10" cast "int" as "pr10",
                $"pr15" cast "int" as "pr15",
                $"pr20" cast "int" as "pr20",
                $"prog_g_index" cast "int" as "prog_g_index",
                $"prog_name",
                $"prog_genres"
              )

            val groupedDF = results.filter(r=> r.getString(13)!=null && !r.getString(13).isEmpty)
              .flatMap(r=>{
                val genres = r.getString(13).split(",")
                genres.map(g=>{
                  (
                    r.getInt(1), // gr_pr1
                    r.getInt(2), // gr_pr5
                    r.getInt(3), // gr_pr10
                    r.getInt(4), // gr_pr15
                    r.getInt(5), // gr_pr20
                    r.getInt(6), // pr1
                    r.getInt(7), // pr5
                    r.getInt(8), // pr10
                    r.getInt(9), // pr15
                    r.getInt(10), // pr20
                    g // genre
                  )
                })
              })
              .toDF("gr_pr1","gr_pr5","gr_pr10","gr_pr15","gr_pr20","pr1","pr5","pr10","pr15","pr20","genre")
              .groupBy("genre")
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

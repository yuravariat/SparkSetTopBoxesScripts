package SetTopBoxes.Trash

import java.io.Serializable
import java.text.SimpleDateFormat

import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ListBuffer

//<editor-fold desc="Case classes">

case class PredictResult(
                          device_id: String,
                          clust_nums: String, //Seq[Int],
                          views_count: Int,
                          guesses: Int,
                          translatedProgsAvg: Double,
                          group_Precision_At1: Double,
                          group_Precision_At5: Double,
                          group_Precision_At10: Double,
                          group_Precision_At15: Double,
                          group_Precision_At20: Double,
                          precision_At1: Double,
                          precision_At5: Double,
                          precision_At10: Double,
                          precision_At15: Double,
                          precision_At20: Double) extends Serializable {
}

case class StatResult(
                       category: String,
                       group_Precision_At1: Double,
                       group_Precision_At5: Double,
                       group_Precision_At10: Double,
                       group_Precision_At15: Double,
                       group_Precision_At20: Double,
                       precision_At1: Double,
                       precision_At5: Double,
                       precision_At10: Double,
                       precision_At15: Double,
                       precision_At20: Double) extends Serializable {
}


//</editor-fold>

object ProgPredictResultsExplore {

  val spark: SparkSession = SparkSession
    .builder()
    .master("local")
    .appName("Scala Spark SQL")
    .config("spark.sql.warehouse.dir", "file:///tmp/spark-warehouse")
    .getOrCreate()

  import spark.implicits._

  def main(args: Array[String]): Unit = {

    println("#" * 70 + " ProgPredictResultsExplore " + "#" * 70)

    // Map cityname => (timediff, k number)
    val cities = Map[String, (Int, Int)]("Amarillo" -> (5, 12), "Parkersburg" -> (4, 12), "Little Rock-Pine Bluff" -> (5, 12), "Seattle-Tacoma" -> (7, 12))

    val partsOfDay = 1
    println("#" * 50 + " parts of days " + partsOfDay + " " + "#" * 50)

    val StatResults = ListBuffer[StatResult]()
    val format = new SimpleDateFormat("yyyyMMddHHmmss")
    val simpleFormat = new SimpleDateFormat("dd-MM-yyyy")
    val local = false
    var basePath = ""
    var I = ""

    if (local) {
      basePath = "D:\\SintecMediaData\\"
      I = "\\"
    }
    else {
      basePath = "s3n://magnet-fwm/home/TsviKuflik/"
      I = "/"
    }
    for (citykey <- cities.keys) {

      //<editor-fold desc="City vars">

      val city_name = citykey
      val city_name_safe = city_name.replace(" ", "-")
      val clusters_num = cities(city_name)._2
      val results_csv_path = basePath + "y_predictions" + I + "5_months" + I + "vect_" + partsOfDay + "_12_raw" + I + city_name_safe

      val base_file_name = I + "5_months" + I + city_name_safe + "_vect_" + partsOfDay
      val results_path = basePath + "y_predictions" + base_file_name + I + "k_" + clusters_num + I + "part*"

      println("working on " + city_name)
      println("results_path=" + results_path)

      //</editor-fold>

      //    val data = sc.textFile(results_path)
      //      .map(s=>{
      //        s.replaceAll("""ArrayBuffer\((.*)\)""","\"ArrayBuffer($1)\"")
      //      })


      val results = spark.read.csv(results_path)
        .select(
          $"_c0" as "device_id",
          $"_c1" as "clust_nums",
          $"_c2" cast "int" as "views_count",
          $"_c3" cast "int" as "guesses",
          $"_c4" cast "double" as "translatedProgsAvg",
          $"_c5" cast "double" as "group_Precision_At1",
          $"_c6" cast "double" as "group_Precision_At5",
          $"_c7" cast "double" as "group_Precision_At10",
          $"_c8" cast "double" as "group_Precision_At15",
          $"_c9" cast "double" as "group_Precision_At20",
          $"_c10" cast "double" as "precision_At1",
          $"_c11" cast "double" as "precision_At5",
          $"_c12" cast "double" as "precision_At10",
          $"_c13" cast "double" as "precision_At15",
          $"_c14" cast "double" as "precision_At20"
        )
        .orderBy($"group_Precision_At1")
        .as[PredictResult]

      println(city_name + " reults count " + results.count())

      results
        .coalesce(1)
        .write
        .option("header", "true")
        .csv(results_csv_path)

    }

    println("Done!")
  }
}

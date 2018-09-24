package SetTopBoxes.Trash

import java.io.Serializable
import java.net.URI

import SetTopBoxes.Trash
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

//<editor-fold desc="Case classes">

case class PredictResult(
                          device_id: String,
                          cluster_num: String, //Seq[Int],
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
case class CityData(
                     hours_diff: Int,
                     cluster_nums: scala.collection.mutable.Map[Int, Int]
                   ) extends Serializable

//</editor-fold>

object ProgPredictResultsShow {
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

    println("\n"+"#" * 70 + " ProgPredictResultsShow " + "#" * 70 + "\n")

    // Map cityname => (timediff, k number)
    val cities = List("Amarillo","Parkersburg","Little Rock-Pine Bluff","Seattle-Tacoma")

    val partsOfDay = 1
    val isGenres = false
    val useDataWithTimeDecay = true
    println("#" * 50 + " parts of days " + partsOfDay + " " + "#" * 50)

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

    val results_csv_path = basePath + "y_predictions" + I + "5_months" +
          (if (useDataWithTimeDecay) "_with_decay" else "") +
          (if (isGenres) "_gen" else "") + I + "vect_" + partsOfDay

    println("basePath=" + basePath)
    println("results_csv_path=" + results_csv_path)
    println("useDataWithTimeDecay=" + useDataWithTimeDecay)

    val stat_results = ListBuffer[Trash.StatResult]()

    for (city_name <- cities) {

      //<editor-fold desc="City vars">

      val city_name_safe = city_name.replace(" ", "-")
      //val clusters_num = cities(city_name)._2
      stat_results.append(Trash.StatResult(city_name, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0))

      val base_file_name = I + "5_months" +
        (if (useDataWithTimeDecay) "_with_decay" else "") +
        (if (isGenres) "_gen" else "") + I + city_name_safe + "_vect_" + partsOfDay

      //val results_path = basePath + "y_predictions" + base_file_name + I + "k_" + clusters_num + I + "part*"
      val results_path = basePath + "y_predictions" + base_file_name + I + "part*"

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
          $"_c1" as "cluster_num",
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
        .as[PredictResult]

      println(city_name + " reults count " + results.count())
      //results.show()

      val desc = results.describe("group_Precision_At1", "group_Precision_At5", "group_Precision_At10", "group_Precision_At15", "group_Precision_At20",
        "precision_At1", "precision_At5", "precision_At10", "precision_At15", "precision_At20")
        .select(
          $"summary" as "category",
          $"group_Precision_At1" cast "double",
          $"group_Precision_At5" cast "double",
          $"group_Precision_At10" cast "double",
          $"group_Precision_At15" cast "double",
          $"group_Precision_At20" cast "double",
          $"precision_At1" cast "double",
          $"precision_At5" cast "double",
          $"precision_At10" cast "double",
          $"precision_At15" cast "double",
          $"precision_At20" cast "double"
        )
        .as[Trash.StatResult]

      desc.show()
      stat_results.append(desc.collect(): _*)
    }

    if(FileSystem.get(new URI("s3n://magnet-fwm"), sc.hadoopConfiguration).exists(new Path(results_csv_path)))
    {
      println("Deleting " + results_csv_path)
      FileSystem.get(new URI("s3n://magnet-fwm"), sc.hadoopConfiguration).delete(new Path(results_csv_path), true)
    }

    stat_results.toDF()
      .coalesce(1)
      .write
      .option("header", "true")
      .csv(results_csv_path)

    println("Done!")

    //</editor-fold>
  }
}

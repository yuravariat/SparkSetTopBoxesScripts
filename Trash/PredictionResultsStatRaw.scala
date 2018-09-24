package SetTopBoxes.Trash

import java.io.Serializable
import java.net.URI

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Encoder, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

//<editor-fold desc="Case classes">

case class PredictResultAttemp(
                                device_id: String,
                                city: String,
                                found: Int,
                                gr_found: Int,
                                program_position: Int,
                                gr_program_position: Int,
                                cluster_num: Int,
                                total_cls_num: Int,
                                part: Int,
                                event_date_time: java.sql.Timestamp,
                                prog_code: String,
                                translated_progs_num: Int
                              ) extends Serializable {
  override def toString: String =
    s"$device_id|$city|$found|$gr_found|$program_position|$gr_program_position|" +
      s"$cluster_num|$total_cls_num|$part|$event_date_time|$prog_code|$translated_progs_num"
}

case class PredictResultSummary(
                                 var city: String,
                                 var part: Int,
                                 var views: Int,
                                 var clusters:Int,
                                 var translatedProgsAvg: Double,
                                 var group_Precision_At1: Double,
                                 var group_Precision_At5: Double,
                                 var group_Precision_At10: Double,
                                 var group_Precision_At15: Double,
                                 var group_Precision_At20: Double,
                                 var precision_At1: Double,
                                 var precision_At5: Double,
                                 var precision_At10: Double,
                                 var precision_At15: Double,
                                 var precision_At20: Double) extends Serializable {
}


//</editor-fold>

object PredictionResultsStatRaw {
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

    val partsOfDay = 1
    val isGenres = false
    val useDataWithTimeDecay = true
    val predictOnlyOnTuningEvent = true

    val basePath = "s3n://magnet-fwm/home/TsviKuflik/"
    val results_base_path = basePath + "y_predictions/5_months" +
      (if (useDataWithTimeDecay) "_with_decay" else "") +
      (if (isGenres) "_gen" else "") + (if (predictOnlyOnTuningEvent) "_on_tuning" else "")

    val destination_base_path = basePath + "y_predictions/5_months" +
      (if (useDataWithTimeDecay) "_with_decay" else "") +
      (if (isGenres) "_gen" else "") + (if (predictOnlyOnTuningEvent) "_on_tuning" else "") +
      "/raw_" + partsOfDay + "/"

    println("cities=" + cities.mkString(", "))
    println("partsOfDay=" + partsOfDay)
    println("isGenres=" + isGenres)
    println("useDataWithTimeDecay=" + useDataWithTimeDecay)
    println("predictOnlyOnTuningEvent=" + predictOnlyOnTuningEvent)
    println("basePath=" + basePath)
    println("results_base_path=" + results_base_path)
    println("destination_base_path=" + destination_base_path)
    println("\n")

    var results_paths = ListBuffer[String]()
    for (city_name <- cities) {
      val city_name_safe = city_name.replace(" ", "-")
      val path = results_base_path + "/" + city_name_safe + "_raw_" + partsOfDay + "/part*"
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
        $"_c11" cast "int" as "translated_progs_num"
      )
      .as[PredictResultAttemp]

    //results.show()

    val aggregator = new Aggregator[PredictResultAttemp, PredictResultSummary, PredictResultSummary] with Serializable {
      override def zero: PredictResultSummary = PredictResultSummary("", 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)
      override def reduce(s: PredictResultSummary, a: PredictResultAttemp): PredictResultSummary = {
        if (s.city.isEmpty) {
          s.city = a.city
        }
        if (s.part == 0) {
          s.part = a.part
        }
        if(s.clusters == 0){
          s.clusters = a.total_cls_num
        }
        s.views += 1
        s.translatedProgsAvg += a.translated_progs_num
        s.group_Precision_At1 += (if (a.gr_program_position == 0) 1 else 0)
        s.group_Precision_At5 += (if (a.gr_program_position >= 0 && a.gr_program_position <= 5) 1 else 0)
        s.group_Precision_At10 += (if (a.gr_program_position >= 0 && a.gr_program_position <= 10) 1 else 0)
        s.group_Precision_At15 += (if (a.gr_program_position >= 0 && a.gr_program_position <= 15) 1 else 0)
        s.group_Precision_At20 += (if (a.gr_program_position >= 0 && a.gr_program_position <= 20) 1 else 0)
        s.precision_At1 += (if (a.program_position == 0) 1 else 0)
        s.precision_At5 += (if (a.program_position >= 0 && a.program_position <= 5) 1 else 0)
        s.precision_At10 += (if (a.program_position >= 0 && a.program_position <= 10) 1 else 0)
        s.precision_At15 += (if (a.program_position >= 0 && a.program_position <= 15) 1 else 0)
        s.precision_At20 += (if (a.program_position >= 0 && a.program_position <= 20) 1 else 0)
        s
      }
      override def merge(s1: PredictResultSummary, s2: PredictResultSummary): PredictResultSummary = {
        PredictResultSummary(
          if (!s1.city.isEmpty) s1.city else s2.city,
          if (s1.part > 0) s1.part else s2.part,
          s1.views + s2.views,
          if (s1.clusters > 0) s1.clusters else s2.clusters,
          s1.translatedProgsAvg + s2.translatedProgsAvg,
          s1.group_Precision_At1 + s2.group_Precision_At1,
          s1.group_Precision_At5 + s2.group_Precision_At5,
          s1.group_Precision_At10 + s2.group_Precision_At10,
          s1.group_Precision_At15 + s2.group_Precision_At15,
          s1.group_Precision_At20 + s2.group_Precision_At20,
          s1.precision_At1 + s2.precision_At1,
          s1.precision_At5 + s2.precision_At5,
          s1.precision_At10 + s2.precision_At10,
          s1.precision_At15 + s2.precision_At15,
          s1.precision_At20 + s2.precision_At20)
      }
      override def finish(s: PredictResultSummary): PredictResultSummary = {
        PredictResultSummary(s.city, s.part, s.views, s.clusters,
          s.translatedProgsAvg / s.views.toDouble,
          s.group_Precision_At1 / s.views.toDouble,
          s.group_Precision_At5 / s.views.toDouble,
          s.group_Precision_At10 / s.views.toDouble,
          s.group_Precision_At15 / s.views.toDouble,
          s.group_Precision_At20 / s.views.toDouble,
          s.precision_At1 / s.views.toDouble,
          s.precision_At5 / s.views.toDouble,
          s.precision_At10 / s.views.toDouble,
          s.precision_At15 / s.views.toDouble,
          s.precision_At20 / s.views.toDouble)
      }
      override def bufferEncoder: Encoder[PredictResultSummary] = ExpressionEncoder()
      override def outputEncoder: Encoder[PredictResultSummary] = ExpressionEncoder()
    }.toColumn

    val byPartsOfDays = results
      .groupByKey(c => c.city + c.part)
      .agg(aggregator)
      .map(t => t._2)
      .sort($"city",$"part")
      .toDF()
      .cache()

    byPartsOfDays.show(50)
    val by_parts_save_to = destination_base_path + "by_parts"
    if(FileSystem.get(new URI("s3n://magnet-fwm"), sc.hadoopConfiguration).exists(new Path(by_parts_save_to)))
    {
      println("Deleting " + by_parts_save_to)
      FileSystem.get(new URI("s3n://magnet-fwm"), sc.hadoopConfiguration).delete(new Path(by_parts_save_to), true)
    }
    byPartsOfDays
      .coalesce(1)
      .write
      .option("header", "true")
      .csv(by_parts_save_to)

    val byCities = byPartsOfDays
      .groupBy($"city")
      .agg(
        avg($"translatedProgsAvg") as "translatedProgsAvg",
        avg($"group_Precision_At1") as "group_Precision_At1",
        avg($"group_Precision_At5") as "group_Precision_At5",
        avg($"group_Precision_At15") as "group_Precision_At15",
        avg($"group_Precision_At20") as "group_Precision_At20",
        avg($"precision_At1") as "precision_At1",
        avg($"precision_At5") as "precision_At5",
        avg($"precision_At15") as "precision_At15",
        avg($"precision_At20") as "precision_At20"
      )
      .cache()

    byCities.show()
    val by_cities_save_to = destination_base_path + "by_cities"
    if(FileSystem.get(new URI("s3n://magnet-fwm"), sc.hadoopConfiguration).exists(new Path(by_cities_save_to)))
    {
      println("Deleting " + by_cities_save_to)
      FileSystem.get(new URI("s3n://magnet-fwm"), sc.hadoopConfiguration).delete(new Path(by_cities_save_to), true)
    }
    byCities
      .coalesce(1)
      .write
      .option("header", "true")
      .csv(by_cities_save_to)

    println("Done!")

    //</editor-fold>
  }
}

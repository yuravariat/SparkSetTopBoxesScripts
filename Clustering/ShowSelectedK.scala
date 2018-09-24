package SetTopBoxes.Clustering

import java.io.Serializable

import SetTopBoxes.Trash
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

//<editor-fold desc="Case classes">

case class CityData(
                     hours_diff: Int,
                     cluster_nums: scala.collection.mutable.Map[Int, Int]
                   ) extends Serializable

//</editor-fold>

object ShowSelectedK {
  @transient val conf: SparkConf = new SparkConf().setMaster("local").setAppName("Devices")
  @transient val sc: SparkContext = new SparkContext(conf)
  val spark: SparkSession = SparkSession
    .builder()
    .master("local")
    .appName("Scala Spark SQL")
    .config("spark.sql.warehouse.dir", "file:///tmp/spark-warehouse")
    .getOrCreate()

  def main(args: Array[String]): Unit = {

    //<editor-fold desc="Run area">

    println("\n"+"#" * 70 + " Show Selected K values " + "#" * 70 + "\n")
    val local = false
    val isGenres = false
    val useDataWithTimeDecay = true
    val timeDecayRate = "_30"
    val OnlyOnTuningEvent = false
    val div5new = true
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

    val parts = List(10)

    println("basePath=" + basePath)
    println("isGenres=" + isGenres)
    println("useDataWithTimeDecay=" + useDataWithTimeDecay)
    println("timeDecayRate=" + timeDecayRate)
    println("div5new=" + div5new)
    println("OnlyOnTuningEvent=" + OnlyOnTuningEvent)

    for( partsOfDay <- parts ){

      println("#" * 50 + " parts of days " + partsOfDay + " " + "#" * 50)

      val cities = Map[String, CityData]("Amarillo" -> CityData(5, scala.collection.mutable.Map[Int, Int]()), "Parkersburg" -> CityData(4, scala.collection.mutable.Map[Int, Int]()),
        "Little Rock-Pine Bluff" -> CityData(5, scala.collection.mutable.Map[Int, Int]()), "Seattle-Tacoma" -> CityData(7, scala.collection.mutable.Map[Int, Int]()))

      for (city_name <- cities.keys) {
        val city_name_safe = city_name.replace(" ", "-")
        for (pOfDay <- 1 until (partsOfDay + 1)) {

          val clust_results_path_base = basePath + "y_clustering_results" + I + "5_months" +
            (if (isGenres) "_gen" else "") + (if (useDataWithTimeDecay) "_with_decay" + timeDecayRate else "") +
            (if (OnlyOnTuningEvent) "_on_tuning" else "") + I + city_name_safe + "_vect_" + partsOfDay + I
          val selected_k_path = clust_results_path_base +
            (if (partsOfDay == 1) "selected_k" else "part" + pOfDay + (if (div5new) "_v2" else "") + I + "selected_k") + I + "part*"

          val k = sc.textFile(selected_k_path).map(s => s.split(",")(0).toInt).take(1).head
          cities(city_name).cluster_nums += pOfDay -> k
        }
      }

      for (city_name <- cities.keys) {
        println(city_name)
        for(pOfDay<-cities(city_name).cluster_nums.keys){
          println("part " + pOfDay + " k=" + cities(city_name).cluster_nums(pOfDay))
        }
      }
    }

    println("Done!")

    //</editor-fold>

  }
}

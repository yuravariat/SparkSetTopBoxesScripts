package SetTopBoxes.Clustering

import java.io.Serializable
import java.net.URI

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object ChoseKValue {

  @transient val conf: SparkConf = new SparkConf().setMaster("local").setAppName("Devices")
  @transient val sc: SparkContext = new SparkContext(conf)
  val spark: SparkSession = SparkSession
    .builder()
    .master("local")
    .appName("Scala Spark SQL")
    .config("spark.sql.warehouse.dir", "file:///tmp/spark-warehouse")
    .getOrCreate()

  import spark.implicits._

  //<editor-fold desc="Case classes">

  case class ClustResult(
                          var K: Int,
                          var WSSSE: Double,
                          var SilScore: Double
                        ) extends Serializable
  case class CityData(
                       cluster_nums: scala.collection.mutable.Map[Int, Int]
                     ) extends Serializable

  //</editor-fold>

  def main(args: Array[String]): Unit = {

    //<editor-fold desc="Run area">

    import org.apache.spark.sql.functions._

    println("\n" + "#" * 70 + " Chose K Value " + "#" * 70 + "\n")

    val cities = List("Amarillo", "Parkersburg", "Little Rock-Pine Bluff", "Seattle-Tacoma")
    val partsOfDayList = List(10)
    val isGenres = false
    val useScaleOf10Genres = true
    val useDataWithTimeDecay = true
    val timeDecayRate = "_30"
    val OnlyOnTuningEvent = false
    val div5new = false
    val local = false
    var basePath = ""
    val showResults = true
    var I = ""

    if (local) {
      basePath = "D:\\SintecMediaData\\"
      I = "\\"
    }
    else {
      basePath = "s3n://magnet-fwm/home/TsviKuflik/"
      I = "/"
    }

    println("partsOfDayList=" + partsOfDayList.mkString(","))
    println("isGenres=" + isGenres)
    println("OnlyOnTuningEvent=" + OnlyOnTuningEvent)
    println("useDataWithTimeDecay=" + useDataWithTimeDecay)
    println("timeDecayRate=" + timeDecayRate)
    println("div5new=" + div5new)
    println("useScaleOf10Genres=" + useScaleOf10Genres)

    for(partsOfDay<-partsOfDayList) {

      println("#" * 50 + " parts of days " + partsOfDay + " " + "#" * 50)

      for (city_name <- cities) {

        println("#" * 50 + " " + city_name  + " " + "#" * 50)

        val city_name_safe = city_name.replace(" ", "-")
        for (pOfDay <- 1 until (partsOfDay + 1)) {

          println("#" * 50 + " part " + pOfDay + " " + "#" * 50)

          val clust_results_path_base = basePath + "y_clustering_results" + I + "5_months" +
            (if (isGenres) "_gen" else "") +
            (if (useDataWithTimeDecay) "_with_decay" + timeDecayRate else "") +
            (if (OnlyOnTuningEvent) "_on_tuning" else "") +
            (if (useScaleOf10Genres) "_scale10" else "") +
             I + city_name_safe + "_vect_" + partsOfDay + I

          val clust_results_path = clust_results_path_base +
            (if (partsOfDay == 1) "runs" else "part" + pOfDay + (if (div5new) "_v2" else "") + I + "runs") + I + "part*"

          val selected_k_path = clust_results_path_base +
            (if (partsOfDay == 1) "selected_k" else "part" + pOfDay + (if (div5new) "_v2" else "") + I + "selected_k")

          val run_results = sc.textFile(clust_results_path)
            .filter(s => {
              !s.isEmpty
            })
            .map(s => {
              val sp = s.split(",")
              ClustResult(sp(0).toInt, sp(1).toDouble, sp(2).toDouble)
            })
            .filter(r =>r.K > 5)
            .toDF()

          run_results.show(50)
          //run_results.describe("WSSSE", "SilScore").show()

          var silScoreMax = 0.0
          var silScoreStd = 0.0
          var selectedClustResult = ClustResult(5, 0, 0)

          run_results
            .select(
              max($"SilScore"),
              stddev($"SilScore")
            )
            .take(1).foreach(r => {
            silScoreMax = r.getDouble(0)
            silScoreStd = r.getDouble(1)
          })
          val lowLimit = silScoreMax - silScoreStd/2

          run_results.collect().foreach(r => {
            if (r.getDouble(2) > lowLimit && r.getInt(0)>5 && r.getInt(0)<100 &&
              (selectedClustResult.WSSSE==0 || selectedClustResult.WSSSE > r.getDouble(1))
              //|| (selectedClustResult.WSSSE!=0 && r.getDouble(1)-selectedClustResult.WSSSE < 4)
            ) {
              selectedClustResult.K = r.getInt(0)
              selectedClustResult.WSSSE = r.getDouble(1)
              selectedClustResult.SilScore = r.getDouble(2)
            }
          })

          if(FileSystem.get(new URI("s3n://magnet-fwm"), sc.hadoopConfiguration).exists(new Path(selected_k_path)))
          {
            println("Deleting " + selected_k_path)
            FileSystem.get(new URI("s3n://magnet-fwm"), sc.hadoopConfiguration).delete(new Path(selected_k_path), true)
          }

          println("selected k is " + selectedClustResult.K + " WSSSE=" + selectedClustResult.WSSSE + " sil=" + selectedClustResult.SilScore)
          sc.parallelize(List(selectedClustResult))
            .map(r => r.K + "," + r.WSSSE + "," + r.SilScore)
            .coalesce(1)
            .saveAsTextFile(selected_k_path)

          //println("perss any ke to continue")
          //System.in.read()
          //println("key pressed")
        }
      }
      if(showResults){

        val cities_map = cities.map(c=>(c,CityData(scala.collection.mutable.Map[Int, Int]()))).toMap

        for (city_name <- cities_map.keys) {
          val city_name_safe = city_name.replace(" ", "-")
          for (pOfDay <- 1 until (partsOfDay + 1)) {

            val clust_results_path_base = basePath + "y_clustering_results" + I + "5_months" +
              (if (isGenres) "_gen" else "") + (if (useDataWithTimeDecay) "_with_decay" + timeDecayRate else "") +
              (if (OnlyOnTuningEvent) "_on_tuning" else "") + I + city_name_safe + "_vect_" + partsOfDay + I
            val selected_k_path = clust_results_path_base +
              (if (partsOfDay == 1) "selected_k" else "part" + pOfDay + (if (div5new) "_v2" else "") + I + "selected_k") + I + "part*"

            val k = sc.textFile(selected_k_path).map(s => s.split(",")(0).toInt).take(1).head
            cities_map(city_name).cluster_nums += pOfDay -> k
          }
        }

        for (city_name <- cities_map.keys) {
          println(city_name)
          for(pOfDay<-cities_map(city_name).cluster_nums.keys){
            println("part " + pOfDay + " k=" + cities_map(city_name).cluster_nums(pOfDay))
          }
        }
      }
    }
    println("Done!")

    //</editor-fold>
  }
}

package SetTopBoxes.DemoData

import java.net.URI
import java.text.SimpleDateFormat
import java.util.concurrent.TimeUnit

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.compress.GzipCodec
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.udf
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
  * Created by yurav on 19/04/2017.
  */
object GetDevices {

  val spark: SparkSession = SparkSession
    .builder()
    .master("local")
    .appName("Scala Spark SQL")
    .config("spark.sql.warehouse.dir", "file:///tmp/spark-warehouse")
    .getOrCreate()

  import spark.implicits._

  //<editor-fold desc="Classes">

  //</editor-fold>

  /** Main function */
  def main(args: Array[String]): Unit = {
    @transient val conf: SparkConf = new SparkConf().setMaster("local").setAppName("Devices")
    @transient val sc: SparkContext = new SparkContext(conf)

    //<editor-fold desc="Run area">

    println("\n" + "#" * 70 + " Get Devices For Demographic Analysis " + "#" * 70 + "\n")

    val basePath = "s3n://magnet-fwm/home/TsviKuflik/"
    val cities = List[String]("Amarillo", "Parkersburg", "Little Rock-Pine Bluff", "Seattle-Tacoma")
    val partsOfDayList = List(1, 8)
    val useDataWithTimeDecayList = List(true, false)
    val isGenres = false
    val months = 5

    println("cities=" + cities.mkString(", "))
    println("partsOfDayList=" + partsOfDayList.mkString(", "))
    println("useDataWithTimeDecayList=" + useDataWithTimeDecayList.mkString(", "))


    for (partsOfDay <- partsOfDayList) {
      for (useDataWithTimeDecay <- useDataWithTimeDecayList) {
            //<editor-fold desc="work">

            //val results_paths: ListBuffer[String] = ListBuffer[String]()
            var clustered_data_container:RDD[String] = null

            for (citykey <- cities) {

              val city_name = citykey
              val city_name_safe = city_name.replace(" ", "-")
              val clustered_data_base_path = basePath + "y_clustering_results/" + months + "_months" + (if (useDataWithTimeDecay) "_with_decay" else "") +
              (if (isGenres) "_gen" else "") + "/" + city_name_safe + "_vect_" + partsOfDay

              println("partsOfDay=" + partsOfDay)
              println("isGenres=" + isGenres)
              println("useDataWithTimeDecay=" + useDataWithTimeDecay)
              println("clustered_data_base_path=" + clustered_data_base_path)
              println("\n")

              for (pOfDay <- 1 until (partsOfDay + 1)) {

                val selected_k_path = basePath + "y_clustering_results/" + months + "_months" + (if (useDataWithTimeDecay) "_with_decay" else "") +
                  "/" + city_name_safe + "_vect_" + partsOfDay + "/" +
                  (if (partsOfDay == 1) "selected_k" else "part" + pOfDay + "/selected_k") + "/part*"

                val chosenK = sc.textFile(selected_k_path).map(s => s.split(",")(0).toInt).take(1).head
                val clustered_data_path = clustered_data_base_path + (if (partsOfDay == 1) "" else "/part" + pOfDay) +
                                          "/k_" + chosenK + "/clustered/part*"

                //println("clustered_data_path=" + clustered_data_path)

                //val results_path = basePath + "y_devices/for_demo_analysis_" + months + "_months" +
                //  (if (isGenres) "_gen" else "") + (if (useDataWithTimeDecay) "_with_decay" else "") + "/" + city_name_safe + "_" + partsOfDay +
                //  (if (partsOfDay > 1) "part" + pOfDay else "")

                //println("results_path=" + results_path)

                //if (FileSystem.get(new URI("s3n://magnet-fwm"), sc.hadoopConfiguration).exists(new Path(results_path))) {
                //  println("Deleting " + results_path)
                //  FileSystem.get(new URI("s3n://magnet-fwm"), sc.hadoopConfiguration).delete(new Path(results_path), true)
                //}

                //println("Writing to " + results_path)
                //results_paths.append(results_path + "/part*")

                val head = sc.textFile(clustered_data_path).first
                val clustered_data = sc.textFile(clustered_data_path)
                  .filter(s => s != head)
                  .map(s => {
                    // (cluster number, part, device_id, vector )
                    val sp = s.split(",")
                    // (device_id, total_clusters, cluster number, total_parts, part, area, useDataWithTimeDecay )
                    sp(2).replace("\"", "") + "," + chosenK + "," + sp(0) + "," + partsOfDay + "," + sp(1) + "," + city_name_safe + "," + useDataWithTimeDecay
                  })
                  //.saveAsTextFile(results_path, classOf[GzipCodec])
                if(clustered_data_container==null){
                  clustered_data_container = clustered_data
                }
                else{
                  clustered_data_container = clustered_data_container.union(clustered_data)
                }
              }

            }

            println("\n" + "#" * 70 + " Combine all, prepare for SQL server... " + "#" * 70 + "\n")

            val combined_results_path = basePath + "y_devices/for_demo_analysis/" + months + "_months" +
              (if (isGenres) "_gen" else "") + (if (useDataWithTimeDecay) "_with_decay" else "") + "/Combined_" + partsOfDay

            if (FileSystem.get(new URI("s3n://magnet-fwm"), sc.hadoopConfiguration).exists(new Path(combined_results_path))) {
              println("Deleting " + combined_results_path)
              FileSystem.get(new URI("s3n://magnet-fwm"), sc.hadoopConfiguration).delete(new Path(combined_results_path), true)
            }

            println("Writing to " + combined_results_path)
            //val all_paths = String.join(",", results_paths: _*)
            //println("all paths: " + all_paths)
            //sc.textFile(all_paths)
            clustered_data_container
              .map(s => {
                // (device_id, total_clusters, cluster number, total_parts, part, area, useDataWithTimeDecay )
                val sp = s.split(",")
                "INSERT INTO [For_Demo_Analysis] VALUES ('" + sp(0) + "'," + sp(1) + "," + sp(2) + "," + sp(3) + "," + sp(4) +
                  ",'" + sp(5) + "'," + (if (sp(6) == "true") "1" else "0") + ");"
              })
              .coalesce(1)
              .saveAsTextFile(combined_results_path, classOf[GzipCodec])

            println("\n" + "#" * 70 + " Combine all, prepare for SQL server Done" + "#" * 70 + "\n")

            //</editor-fold>
      }
    }

    println("done!")

    //</editor-fold>
  }
}
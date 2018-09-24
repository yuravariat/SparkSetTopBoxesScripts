package SetTopBoxes.Clustering

import java.net.URI
import java.text.SimpleDateFormat
import java.util.Calendar
import java.util.concurrent.TimeUnit

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.compress.GzipCodec
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.rdd.RDD

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object KMeansRun {

  @transient val conf: SparkConf = new SparkConf().setMaster("local").setAppName("Devices")
  @transient val sc: SparkContext = new SparkContext(conf)

  //<editor-fold desc="Classes">

  object Metrix extends Serializable {
    private final val sqr = (x: Double) => x * x

    @throws(classOf[IllegalArgumentException])
    def manhattan[T, U](x: Array[T], y: Array[U])(implicit f: T => Double, g: U => Double): Double = {
      require(x.length == y.length,
        s"Distance.manhattan Vectors have different size ${x.length} and ${y.length}")

      (x, y).zipped.map { case (u, v) => Math.abs(u - v) }.sum
    }

    @throws(classOf[IllegalArgumentException])
    def euclidean[T, U](x: Array[T], y: Array[U])(implicit f: T => Double, g: U => Double): Double = {
      require(x.length == y.length,
        s"Distance.euclidean Vectors have different size ${x.length} and ${y.length}")

      Math.sqrt((x, y).zipped.map { case (u, v) => sqr(u - v) }.sum)
    }

    @throws(classOf[IllegalArgumentException])
    def cosine[T, U](x: Array[T], y: Array[U])(implicit f: T => Double, g: U => Double): Double = {
      require(x.length == y.length,
        s"Distance.cosine Vectors have different size ${x.length} and ${y.length}")

      val norms = (x, y).zipped.map { case (u, v) => Array[Double](u * v, u * u, v * v) }
        ./:(Array.fill(3)(0.0))((s, t) => s ++ t)

      norms(0) / Math.sqrt(norms(1) * norms(2))
    }

    def ComputeSilhouetteScore(points: RDD[(Long, String, Int, Array[Int])],
                               affinityMap: Map[Long, mutable.Map[Long, Double]]): Double = {
      val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")

      val pointsByCluster = points.map(p => (p._3, mutable.ListBuffer(p._1))) // (cluster, List of points indices)
        .reduceByKey { case (a, b) => // by cluster num
        for (c <- b) {
          a.append(c)
        }
        a
      }
        .collect().toMap

      //println(format.format(Calendar.getInstance().getTime) + " ComputeSilhouetteScore - pointsByCluster count=" + pointsByCluster.keys.size)

      val pointsSilhouetteScores = ListBuffer[Double]()

      try {
        for (cls <- pointsByCluster.keys) {
          for (point_indx <- pointsByCluster(cls)) {

            // Inner average distance to points in same cluster
            var average_inner_dist: Double = 0.0
            for (other_p <- pointsByCluster(cls)) {
              // If the point is not current point
              if (other_p != point_indx) {
                //average_inner_dist += affinityTable(point_indx.toInt)(other_p.toInt)
                val value = if (point_indx > other_p) affinityMap(point_indx.toInt)(other_p.toInt)
                else affinityMap(other_p.toInt)(point_indx.toInt)
                average_inner_dist += value
              }
            }
            average_inner_dist = average_inner_dist / (if ((pointsByCluster(cls).length - 1) > 0) pointsByCluster(cls).length.toDouble - 1 else 1.0)

            // Average distance to each other cluster, pick the minimal
            var min_outer_average_dist = Double.MaxValue
            for (other_cls <- pointsByCluster.keys) {
              // If this is other cluster
              if (other_cls != cls) {
                var average_outer_dist: Double = 0.0
                // Loop over all points in this cluster and sum their distances to current point
                for (other_cls_point_indx <- pointsByCluster(other_cls)) {
                  //average_outer_dist += affinityTable(point_indx.toInt)(other_cls_point_indx.toInt)
                  val value = if (point_indx > other_cls_point_indx) affinityMap(point_indx.toInt)(other_cls_point_indx.toInt)
                  else affinityMap(other_cls_point_indx.toInt)(point_indx.toInt)
                  average_outer_dist += value
                }
                average_outer_dist = average_outer_dist / pointsByCluster(other_cls).length.toDouble

                if (average_outer_dist < min_outer_average_dist) {
                  min_outer_average_dist = average_outer_dist
                }
              }
            }
            val silhouetteScore = (min_outer_average_dist - average_inner_dist) / Math.max(min_outer_average_dist, average_inner_dist)
            pointsSilhouetteScores.append(silhouetteScore)
          }
        }
        pointsSilhouetteScores.sum / pointsSilhouetteScores.length.toDouble
      }
      catch {
        case e: Throwable => println(e.toString + e.getStackTrace.mkString(","))
          0.0
      }
    }
  }

  //</editor-fold>

  def main(args: Array[String]): Unit = {

    //<editor-fold desc="Run area">

    println("\n" + "#" * 70 + " KMeansRun " + "#" * 70 + "\n")

    val cities = List[String]("Amarillo", "Parkersburg", "Little Rock-Pine Bluff", "Seattle-Tacoma")

    val partsOfDay = 10
    val isGenres = true
    val useDataWithTimeDecay = true
    val timeDecayRate = "_70"
    val OnlyOnTuningEvent = false
    val div5new = false
    val useScaleOf10Genres = true

    val calculateSilhouetteScore = true
    val calculateWSSSE = true
    val saveResults = true
    val saveModel = false
    val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")

    println("#" * 50 + " parts of days " + partsOfDay + " " + "#" * 50)
    val kNums = List(5, 6, 7, 8, 9, 10, 11, 12, 14, 18, 20, 23, 25, 27, 30, 35, 40, 45, 50, 55, 60, 70, 80)

    println("calculateSilhouetteScore=" + calculateSilhouetteScore)
    println("calculateWSSSE=" + calculateWSSSE)
    println("saveResults=" + saveResults)
    println("saveModel=" + saveModel)
    println("format=" + format)
    println("useDataWithTimeDecay=" + useDataWithTimeDecay)
    println("timeDecayRate=" + timeDecayRate)
    println("OnlyOnTuningEvent=" + OnlyOnTuningEvent)
    println("div5new=" + div5new)
    println("useScaleOf10Genres=" + useScaleOf10Genres)

    for (i <- cities.indices) {

      val city_name = cities(i).replace(" ", "-")
      println("Working on " + city_name)

      val basePath = "s3n://magnet-fwm/home/TsviKuflik/y_for_clustering/5_months" + (if (isGenres) "_gen" else "") +
        (if (useDataWithTimeDecay) "_with_decay" + timeDecayRate else "") + (if (OnlyOnTuningEvent) "_on_tuning" else "") +
        (if (useScaleOf10Genres) "_scale10" else "") + "/" + city_name + "_vect_" + partsOfDay

      val resultsBasePath = "s3n://magnet-fwm/home/TsviKuflik/y_clustering_results/5_months" + (if (isGenres) "_gen" else "") +
        (if (useDataWithTimeDecay) "_with_decay" + timeDecayRate else "") + (if (OnlyOnTuningEvent) "_on_tuning" else "") +
        (if (useScaleOf10Genres) "_scale10" else "") + "/" + city_name + "_vect_" + partsOfDay

      println("basePath " + basePath)
      println("resultsBasePath " + resultsBasePath)

      for (pOfDay <- 1 until (partsOfDay + 1)) {

        val matrixSource = basePath + (if (partsOfDay == 1) "/part*" else "/part" + pOfDay + (if (div5new) "_v2" else "") + "/part*")
        println("matrixSource=" + matrixSource)
        var affinityMap: Map[Long, mutable.Map[Long, Double]] = null

        println(city_name + " KMeansRun started part " + pOfDay)
        // k, WSSSE, silhouette
        var runResults: ListBuffer[(Int, Double, Double, Array[(Int, Int)])] =
          ListBuffer[(Int, Double, Double, Array[(Int, Int)])]()

        //<editor-fold desc="Load and parse the data">

        val data = sc.textFile(matrixSource)
        val header = data.first
        val parsedData = data.repartition(200)
          .filter(l => l != header)
          .zipWithIndex()
          .map { case (s, index) =>
            val sp = s.split(",")
            (index, sp(0), sp.slice(1, sp.length).map(_.toInt))
          }
          .cache()

        val vectors = parsedData.map(s => {
          Vectors.dense(s._3.map(_.toDouble))
        }).cache()

        if (calculateSilhouetteScore) {
          println("Computing affinity map for Silhouette score")

          val parsedData2 = parsedData.map(p => (p._1, p._3))
          val allPairs = parsedData2.cartesian(parsedData2)
            .filter(p => p._1._2 != null && p._2._2 != null && p._1._1 > p._2._1)
          //println("all pairs count " + allPairs.count())
          val pointsToCompute = allPairs
            .map(p => {
              var dist: Double = 0
              // Regular way euclidean distance
              //for (i <- p._1._2.indices) {
              //  dist += (p._1._2(i) - p._2._2(i)) * (p._1._2(i) - p._2._2(i))
              //}

              // scikit-learn way euclidean distance
              var XY: Long = 0
              var XX: Long = 0
              var YY: Long = 0
              for (i <- p._1._2.indices) {
                XY += p._1._2(i) * p._2._2(i)
                XX += p._1._2(i) * p._1._2(i)
                YY += p._2._2(i) * p._2._2(i)
              }
              dist = (-2 * XY) + XX + YY
              (p._1._1, p._2._1, Math.sqrt(dist))

              // Cosine distance, / 1000 for bring back normalization from. See matrix creation (*100).
              //val dot = (p._1._2 zip p._2._2).map {
              //  Function.tupled(_ * _ / 1000)
              //}.sum
              //val magnitude1 = math.sqrt(p._1._2.map(i => i * i / 1000).sum)
              //val magnitude2 = math.sqrt(p._2._2.map(i => i * i / 1000).sum)
              //dist = 1.0 - dot / (magnitude1 * magnitude2)
              //(p._1._1, p._2._1, dist)
            })

          affinityMap = pointsToCompute
            .map(p => (p._1, scala.collection.mutable.Map(p._2 -> p._3)))
            .reduceByKey { case (a, b) =>
              for (k <- b.keys) {
                a(k) = b(k)
              }
              a
            }
            .collect().toMap

          println("Computing affinity map for Silhouette score end")

        }
        //</editor-fold>

        val headersRdd = sc.parallelize(List((0, "\"cluster\",\"part\"," + header))) // cluster, device_id, prog_1, prog_4, prog_...
        for (k <- kNums) {

          println(city_name + " running kmeans with k=" + k + " part " + pOfDay)
          val resultsPath = resultsBasePath + (if (partsOfDay == 1) "" else "/part" + pOfDay) + (if (div5new) "_v2" else "") + "/k_" + k
          var silhouetteScore: Double = 0.0
          var WSSSE = 0.0

          // Cluster the data
          val kMeansModel = KMeans.train(vectors, k, maxIterations = 200, initializationMode = KMeans.K_MEANS_PARALLEL)

          if (calculateWSSSE) {
            //Evaluate clustering by computing Within Set Sum of Squared Errors
            //println(city_name + " k=" + k + " calculating WSSSE")
            WSSSE = kMeansModel.computeCost(vectors)
            println("WSSSE = " + WSSSE)
          }

          val predictionsVectors = parsedData
            .map { r =>
              (r._1, r._2, kMeansModel.predict(Vectors.dense(r._3.map(_.toDouble))), r._3)
            }.cache()

          if (calculateSilhouetteScore) {
            //println(city_name + " k=" + k + " calculating SilhouetteScore")
            silhouetteScore = Metrix.ComputeSilhouetteScore(predictionsVectors, affinityMap)
            println("Silhouette Score = " + silhouetteScore)
          }

          if (saveModel) {
            if(FileSystem.get(new URI("s3n://magnet-fwm"), sc.hadoopConfiguration).exists(new Path(resultsPath)))
            {
              println("Deleting " + resultsPath)
              FileSystem.get(new URI("s3n://magnet-fwm"), sc.hadoopConfiguration).delete(new Path(resultsPath), true)
            }
            // Save and load model
            kMeansModel.save(sc, resultsPath)
            //val sameModel = KMeansModel.load(sc, resultsPath)
          }

          if (saveResults) {

            if(FileSystem.get(new URI("s3n://magnet-fwm"), sc.hadoopConfiguration).exists(new Path(resultsPath + "/clustered")))
            {
              println("Deleting " + resultsPath + "/clustered")
              FileSystem.get(new URI("s3n://magnet-fwm"), sc.hadoopConfiguration).delete(new Path(resultsPath + "/clustered"), true)
            }

            predictionsVectors
              // (for order, cluster number, part, device_id, vector )
              .map(r => (1, r._3 + "," + pOfDay + "," + r._2 + "," + r._4.mkString(",")))
              .union(headersRdd)
              .sortByKey()
              .map(v => v._2)
              .coalesce(1)
              .saveAsTextFile(resultsPath + "/clustered", classOf[GzipCodec])

            val clusterCounts = predictionsVectors
              .groupBy(r => r._3) // by cluster number
              .map(r => (r._1, r._2.count(r => 1 == 1)))
              .sortByKey()
              .collect()

            runResults.append((k, WSSSE, silhouetteScore, clusterCounts))
          }
        }
        if (saveResults) {
          val runResultsPath = resultsBasePath + (if (partsOfDay == 1) "" else "/part" + pOfDay) + (if (div5new) "_v2" else "") + "/runs"
          if(FileSystem.get(new URI("s3n://magnet-fwm"), sc.hadoopConfiguration).exists(new Path(runResultsPath)))
          {
            println("Deleting " + runResultsPath)
            FileSystem.get(new URI("s3n://magnet-fwm"), sc.hadoopConfiguration).delete(new Path(runResultsPath), true)
          }

          sc.parallelize(runResults)
            .map(r => r._1 + "," + r._2 + "," + r._3 + "," + r._4.mkString("|"))
            .coalesce(1)
            .saveAsTextFile(runResultsPath)
        }
      }
    }
    println("Done!")
    //</editor-fold>
  }
}

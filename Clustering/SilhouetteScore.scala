package SetTopBoxes.Clustering

import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.hadoop.io.compress.GzipCodec
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object SilhouetteScore {

  @transient val conf: SparkConf = new SparkConf().setMaster("local").setAppName("Devices")
  @transient val sc: SparkContext = new SparkContext(conf)

  object Metrix extends Serializable {

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

      println(format.format(Calendar.getInstance().getTime) + " ComputeSilhouetteScore - pointsByCluster count=" + pointsByCluster.keys.size)

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
                average_inner_dist+=value
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
        case e: Throwable => println(e.toString)
          0.0
      }
    }
  }

  def main(args: Array[String]): Unit = {

    var affinityMap: Map[Long, mutable.Map[Long, Double]] = null
    val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")
    val matrixSource = "s3n://magnet-fwm/home/TsviKuflik/y_for_clustering/6_months/Parkersburg_vect_1_results/k_4/prediction/*.gz"

    val parsedData = sc.textFile(matrixSource)
      .zipWithIndex()
      .map { case (s, index) =>
        val splited = s.split(',')
        (index, splited(0), splited(1).replace("cls_", "").toInt, splited.slice(2, splited.length).map(_.toInt))
      }
      .cache()

    parsedData.take(5).foreach(p => println(p))

    println("Computing affinity map for Silhouette score")

    val parsedData2 = parsedData.map(p => (p._1, p._4))
    val allPairs = parsedData2.cartesian(parsedData2)
      .filter(p => p._1._1 > p._2._1)
    //println("all pairs count " + allPairs.count())
    val pointsToCompute = allPairs
      .map(p => {
        var dist: Long = 0
        // Regular way
        //for (i <- p._1._2.indices) {
        //  dist += (p._1._2(i) - p._2._2(i)) * (p._1._2(i) - p._2._2(i))
        //}

        // scikit-learn way
        var XY: Long = 0
        var XX: Long = 0
        var YY: Long = 0
        for (i <- p._1._2.indices) {
          XY += p._1._2(i) * p._2._2(i)
          XX += p._1._2(i) * p._1._2(i)
          YY += p._2._2(i) * p._2._2(i)
        }
        dist = ((-2 * XY) + XX + YY) / 100 // /100 for simpler sqrt
        (p._1._1, p._2._1, Math.sqrt(dist))
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

    for (i <- 0 to 5) {
      for (j <- 0 to 5) {
        if (i > j) {
          println("affinityMap(" + i + ")(" + j + ")=" + affinityMap(i)(j))
        }
        else if (i < j) {
          println("affinityMap(" + j + ")(" + i + ")=" + affinityMap(j)(i))
        }
      }
    }

    val silhouetteScore = Metrix.ComputeSilhouetteScore(parsedData, affinityMap)

    println("Done!")
  }
}

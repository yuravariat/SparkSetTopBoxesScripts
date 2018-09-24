package SetTopBoxes.Trash

import java.io.Serializable
import java.net.URI
import java.text.SimpleDateFormat
import java.util.concurrent.TimeUnit

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object RunGenrePredict {
  @transient val conf: SparkConf = new SparkConf().setMaster("local").setAppName("Devices")
  @transient val sc: SparkContext = new SparkContext(conf)
  val spark: SparkSession = SparkSession
    .builder()
    .master("local")
    .appName("Scala Spark SQL")
    .config("spark.sql.warehouse.dir", "file:///tmp/spark-warehouse")
    .getOrCreate()

  //<editor-fold desc="Case classes">

  case class Program(
                      prog_id: String,
                      //name: String,
                      //genres: String,
                      //start_date: String,
                      //start_time: String,
                      start_date_time: java.sql.Timestamp,
                      prog_duration: Double
                    ) extends Serializable

  case class ViewingData(
                          //device_id_unique: String,
                          event_date_time: java.sql.Timestamp,
                          //station_num: String,
                          prog_code: String,
                          duration: Int
                          //prog_duration: Int
                        ) extends Serializable

  case class DataPoint(
                        device_id: String,
                        clust_num: Int,
                        part: Int,
                        data: Seq[Double]
                      ) extends Serializable

  case class Prog(
                   codes: Seq[String]
                 ) extends Serializable

  case class RatedGenre(
                         index: Long,
                         rate: Double
                       ) extends Serializable

  case class Device(
                     device_id: String,
                     views: Iterable[ViewingData],
                     genre_rates: Map[Int, (Int, Seq[(String, RatedGenre)])]
                   ) extends Serializable

  class PredictResult(val device_id: String) extends Serializable {
    //def device_id: String
    var clust_nums: Seq[Int] = _
    var views_count: Int = 0
    var guesses: Int = 0
    var translatedProgsAvg: Double = 0
    var group_Precision_At1: Double = 0
    var group_Precision_At5: Double = 0
    var group_Precision_At10: Double = 0
    var group_Precision_At15: Double = 0
    var group_Precision_At20: Double = 0
    var precision_At1: Double = 0
    var precision_At5: Double = 0
    var precision_At10: Double = 0
    var precision_At15: Double = 0
    var precision_At20: Double = 0

    override def toString: String =
      s"$device_id," + "\"" + s"${clust_nums.mkString(",")}" + "\"" + s",$guesses,$views_count,$translatedProgsAvg" +
        s",$group_Precision_At1,$group_Precision_At5,$group_Precision_At10,$group_Precision_At15,$group_Precision_At20" +
        s",$precision_At1,$precision_At5,$precision_At10,$precision_At15,$precision_At20"
  }

  object PartsOfDay extends Serializable {

    import java.util.{Calendar, Locale}

    private val daynameFormat = new SimpleDateFormat("EEEE", Locale.US)

    // 1 parts - simple cluster over all.
    // 4 parts - 4 parts of day, morning, noon, evening, night.
    // 8 parts - 4 parts of day ones for weekdays and ones for weekend.
    // 7 parts - clustering on each of week day separately.
    // 28 parts - 4 parts of day for each week day. 4*7

    //Weekend -> Saturday and Sunday
    def GetPartOfDay(parts: Int, date: java.sql.Timestamp): Int = {
      var partOfDay = 1
      if (parts == 4 || parts == 8 || parts == 28) { // 4 part of days, divided by hours
        val hour = TimeUnit.MILLISECONDS.toHours(date.getTime) % 24
        if (hour >= 6 && hour < 12) {
          partOfDay = 1
        }
        else if (hour >= 12 && hour < 19) {
          partOfDay = 2
        }
        else if ((hour >= 19 && hour <= 23) || hour <= 2) {
          partOfDay = 3
        }
        else {
          partOfDay = 4
        }
      }
      if (parts == 2 || parts == 8) { // parts == 2 => weekdays or weekends, parts == 8 => 4 part of days for weekdays and weekend
        val dayOfWeek = daynameFormat.format(date.getTime)
        if (parts == 2) { //weekdays or weekends
          if (dayOfWeek == "Saturday" || dayOfWeek == "Sunday") {
            partOfDay = 2
          }
          else {
            partOfDay = 1
          }
        }
        else {
          if (dayOfWeek == "Saturday" || dayOfWeek == "Sunday") {
            partOfDay += 4
          }
        }
      }
      if (parts == 7 || parts == 28) { // 7 days
        val cal = Calendar.getInstance
        cal.setTime(date)
        if (parts == 7) {
          partOfDay = cal.get(java.util.Calendar.DAY_OF_WEEK) // day number
        }
        else {
          val dayNum = cal.get(java.util.Calendar.DAY_OF_WEEK) // day number
          partOfDay = (dayNum - 1) * 7 + partOfDay
        }
      }
      partOfDay
    }
  }

  case class CityData(
                       hours_diff: Int,
                       cluster_nums: scala.collection.mutable.Map[Int, Int]
                     ) extends Serializable

  //</editor-fold>

  def main(args: Array[String]): Unit = {

    //<editor-fold desc="Run area">

    println("\n" + "#" * 70 + " RunProgPredict " + "#" * 70 + "\n")

    //<editor-fold desc="Init vars and settings region">

    val partsOfDay = 1
    println("#" * 50 + " parts of days " + partsOfDay + " " + "#" * 50)
    val format = new SimpleDateFormat("yyyyMMddHHmmss")
    val simpleFormat = new SimpleDateFormat("dd-MM-yyyy")
    val predictionStart = new java.sql.Timestamp(format.parse("20150601000000").getTime)
    val predictionEnd = new java.sql.Timestamp(format.parse("20150608000000").getTime)
    val local = false
    val testMode = false
    var basePath = ""
    var I = ""
    val notAllowedPRograms = Array[String]("MP_MISSINGPROGRAM", "DVRPGMKEY"
      , "EP000009937449", "SH000191120000", "SH000000010000", "H002786110000", "SH015815970000", "SH000191160000"
      , "SH000191300000", "SH000191680000", "SH001083440000", "SH003469020000", "SH007919190000", "SH008073210000"
      , "SH014947440000", "SH016938330000", "SH018049520000", "SH020632040000", "SP003015230000")

    if (local) {
      basePath = "D:\\SintecMediaData\\"
      I = "\\"
    }
    else {
      basePath = "s3n://magnet-fwm/home/TsviKuflik/"
      I = "/"
    }
    val programs_path = basePath + "y_programs" + I + "programs_genres_groups" + I + "part-00000"
    val views_path = basePath + "y_views_with_durations"
    //  val translated_programs_path = basePath + "rpt_programs/SintecMedia.rpt_programs.date_2015-05-31*.gz," +
    //    basePath + "rpt_programs/SintecMedia.rpt_programs.date_2015-06*.gz," +
    //    basePath + "rpt_programs/SintecMedia.rpt_programs.date_2015-07-01*.gz"

    val translated_programs_path = basePath + "rpt_programs/SintecMedia.rpt_programs.date_2015-05-31*.gz," +
      basePath + "rpt_programs/SintecMedia.rpt_programs.date_2015-06-01*.gz," +
      basePath + "rpt_programs/SintecMedia.rpt_programs.date_2015-06-02*.gz," +
      basePath + "rpt_programs/SintecMedia.rpt_programs.date_2015-06-03*.gz," +
      basePath + "rpt_programs/SintecMedia.rpt_programs.date_2015-06-04*.gz," +
      basePath + "rpt_programs/SintecMedia.rpt_programs.date_2015-06-05*.gz," +
      basePath + "rpt_programs/SintecMedia.rpt_programs.date_2015-06-06*.gz," +
      basePath + "rpt_programs/SintecMedia.rpt_programs.date_2015-06-07*.gz," +
      basePath + "rpt_programs/SintecMedia.rpt_programs.date_2015-06-08*.gz"

    println("basePath=" + basePath)
    println("programs_path=" + programs_path)
    println("views_path=" + views_path)
    println("translated_programs_path=" + translated_programs_path)

    //</editor-fold>

    //<editor-fold desc="Load cities diff hours and cluster numbers">

    val cities = Map[String, CityData]("Amarillo" -> CityData(5, scala.collection.mutable.Map[Int, Int]()), "Parkersburg" -> CityData(4, scala.collection.mutable.Map[Int, Int]()),
      "Little Rock-Pine Bluff" -> CityData(5, scala.collection.mutable.Map[Int, Int]()), "Seattle-Tacoma" -> CityData(7, scala.collection.mutable.Map[Int, Int]()))

    for (city_name <- cities.keys) {
      val city_name_safe = city_name.replace(" ", "-")
      for (pOfDay <- 1 until (partsOfDay + 1)) {
        val selected_k_path = basePath + "y_clustering_results" + I + "5_months_gen" + I +
          city_name_safe + "_vect_" + partsOfDay + I +
          (if (partsOfDay == 1) "selected_k" else "part" + pOfDay + I + "selected_k" + I + "part*")

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

    //</editor-fold>

    //<editor-fold desc="Programs and translated programs load">

    val GenresArray = sc.textFile("s3n://magnet-fwm/home/TsviKuflik/y_programs/genres/part*")
      .map(_.split("\\|"))
      .filter(s => !s(0).isEmpty)
      .map(s => s(0))
      .collect()
      .sortBy(s => s)
      .toList

    println("RDD programs section")
    val programs = sc.textFile(programs_path)
      .map(s => {
        val sp = s.split("\\|")
        val prog_name = sp(0)
        var genres = Array[String]()
        if (sp.lift(1).isDefined) {
          genres = sp(1).split(",")
        }
        val ids = ListBuffer[String]()
        for (i <- 2 until sp.length) {
          ids.append(sp(i))
        }
        (prog_name, ids, genres)
      })
      .collect()
      .sortBy(s => s._1)
      .toList

    var progsToGenres = programs
      .flatMap(p =>
        p._2.map(prog_id => (prog_id, p._3))
      ).toMap

    println("Translated programs section")
    // Translated Programs RDD[Programs]
    val translatedPrograms = sc.textFile(translated_programs_path)
      .map(s => {
        val sp = s.split("\\|")
        val dt = new java.sql.Timestamp(format.parse(sp(3) + sp(4)).getTime)
        //Program(sp(0), sp(1), sp(2), sp(3), sp(4), dt, sp(5).toDouble)
        Program(sp(0), dt, sp(5).toDouble)
      })
      //.cache()
      .collect()

    //translatedPrograms.take(10).foreach(p=>println(p))

    //</editor-fold>

    for (citykey <- cities.keys) {

      //<editor-fold desc="City vars">

      val city_name = citykey
      val city_name_safe = city_name.replace(" ", "-")

      val base_file_name = I + "5_months_gen" + I + city_name_safe + "_vect_" + partsOfDay
      var clusteredDataPath = ""
      val devices_views_path = basePath + "y_views_for_prediction" + I + "5_months" + I + city_name_safe + "-0601-0608/*.gz"
      val results_path = basePath + "y_predictions" + I + base_file_name

      println("results_path=" + results_path)

      //</editor-fold>

      //<editor-fold desc="Clustered data load">

      // "cluster","part","device_id","genre_14","genre_16","genre_20","genre_39","genre_49"...
      val vectors_headers = mutable.Map[Int, Map[Int, Int]]() // part -> (vector index -> genre index)

      if (partsOfDay > 1) {
        for (pOfDay <- 1 until (partsOfDay + 1)) {
          val clusters_num = cities(city_name).cluster_nums(pOfDay)
          val path = basePath + "y_clustering_results" + base_file_name + I + "part" +
            pOfDay + I + "k_" + clusters_num + I + "clustered" + I + "*.gz"
          clusteredDataPath += path + (if (pOfDay < partsOfDay) "," else "")

          val sp = sc.textFile(path).first().split(",")
          val headers = sp.slice(3, sp.length).map(s => s.replace("\"", "").replace("genre_", "").toInt)
          vectors_headers(pOfDay) = headers.zipWithIndex.map { case (v, indx) => (indx, v) }.toMap
        }
      }
      else {
        val clusters_num = cities(city_name).cluster_nums(1)
        clusteredDataPath = basePath + "y_clustering_results" + base_file_name + I + "k_" + clusters_num + I + "clustered" + I + "*.gz"

        val sp = sc.textFile(clusteredDataPath).first().split(",")
        val headers = sp.slice(3, sp.length).map(s => s.replace("\"", "").replace("genre_", "").toInt)
        vectors_headers(1) = headers.zipWithIndex.map { case (v, indx) => (indx, v) }.toMap
      }

      println("clusteredDataPath=" + clusteredDataPath)
      // Clustered data RDD[DataPoint]
      val clusteredData: RDD[DataPoint] = sc.textFile(clusteredDataPath)
        .repartition(600)
        .filter(s => {
          !s.trim.isEmpty && !s.startsWith("\"")
        })
        .map(s => {
          val sp = s.split(",")
          DataPoint(
            sp(2).replace("\"", ""), // device_id
            sp(0).toInt, // cluster number
            sp(1).toInt, // part
            sp.slice(3, sp.length).map(_.toDouble).toSeq // vector
          )
        })

      //</editor-fold>

      //<editor-fold desc="Rated programs by clusters">

      // Rated programs by cluster. Each cluster and its rates for programs based on means.
      // Map[part,Map[clster_num,List[program code,rate]]]

      // Map[part,Map[clster_num,List[program code,rate]]]
      val rated_genres_by_clusters = clusteredData
        .map(p => (p.part, (p.clust_num, p.data)))
        .groupBy(g => g._1)
        .map(v => {
          val part = v._1
          val vector_headers = vectors_headers(v._2.head._1) // part -> vector index -> prog index
          val clust_vec = v._2.map(g => g._2)
            .groupBy(c => c._1)
            .map(x => {

              val clust = x._1

              // vector that is the sum of all vectors
              val clust_vector = x._2.map(c1 => c1._2)
                .reduce((v1, v2) =>
                  (v2.toArray, v1.toArray).zipped.map(_ + _).toSeq
                )
              // average
              val clust_vector_avg = clust_vector.map(c1 => c1 / x._2.size.toDouble)

              //(clust ,clust_vector_avg)
              // rated programs
              val rated = clust_vector_avg.zipWithIndex
                .map { case (rate, indx) =>
                  val genre_index = vector_headers(indx) // vector index -> prog index
                  (GenresArray(genre_index), RatedGenre(genre_index, rate))
                }
                .filter(v => v._2.rate > 0)
                .sortBy(v => -v._2.rate)

              (clust, rated)
            }
            )
          (part, clust_vec)
        }
        ).collect()
        .toMap

      //</editor-fold>

      //<editor-fold desc="Load devices with views and clusters numbers">

      val devicesViews = sc.textFile(devices_views_path)
        .map(s => {
          val splited = s.split(",").toList
          val device_id = splited.head
          val views = splited.slice(1, s.length)
            .map(v => {
              val sp = v.split("\\|")
              val dt = new java.sql.Timestamp(format.parse(sp(0)).getTime)
              ViewingData(dt, sp(1), sp(2).toInt)
            })
          (device_id, views)
        })

      //println("devicesViews:")
      //devicesViews.collect.foreach(v=>println(v._1 + " -> " + v._2.length))

      // create personal program rates.
      //println("clusteredDataDC:")

      val clusteredDataDC = clusteredData
        .groupBy(d => d.device_id)
        .map(d => {
          val prog_rate_maps = d._2.map(dp => {
            val prog_rate_map = dp.data.zipWithIndex
              .map { case (rate, indx) =>
                val genre_index = vectors_headers(dp.part)(indx) // vector index -> prog index
                (GenresArray(genre_index), RatedGenre(genre_index, rate))
              }
              .sortBy(v => -v._2.rate)

            (dp.part, (dp.clust_num, prog_rate_map))
          }).toMap

          (d._1, prog_rate_maps)
        })

      //clusteredDataDC.foreachPartition(iter => println("clusteredDataDC elements in this partition: " + iter.length))

      val devicesWithViewsAndClusters = devicesViews.join(clusteredDataDC)
        .map(d => {
          // (device id, cluster_num, views, prog rates maps )
          (d._1, Device(d._1, d._2._1, d._2._2))
        })
        .partitionBy(new HashPartitioner(600))
        .persist()
      //.collect()

      //println("devicesWithViewsAndClusters try ...")
      //println("devicesWithViewsAndClusters count=" + devicesWithViewsAndClusters.count())

      //</editor-fold>

      //<editor-fold desc="Prediction">

      println("Starting prediction " + city_name)

      if (!testMode) {
        val predict_results = devicesWithViewsAndClusters
          .mapValues { device =>

            val t0 = System.nanoTime()

            var predict_result = new PredictResult(device_id = device.device_id)
            predict_result.clust_nums = device.genre_rates.map(p => (p._1, p._2._1))
              .toSeq.sortBy(c => c._1)
              .map(c => c._2)

            predict_result.views_count = device.views.size

            //println("#" + index + " device " + device.device_id + " device.views=" + device.views.size)

            for (view <- device.views) {
              if (!notAllowedPRograms.contains(view.prog_code)) {
                predict_result.guesses += 1

                var pOfDay = PartsOfDay.GetPartOfDay(partsOfDay, view.event_date_time)

                val syncedTime = new java.sql.Timestamp(view.event_date_time.getTime + TimeUnit.HOURS.toMillis(cities(city_name).hours_diff))
                val current_clust_num = device.genre_rates(pOfDay)._1

                // Find translated programs
                //println("Find translated programs")
                var trans_progs = translatedPrograms.filter(p => {
                  !notAllowedPRograms.contains(p.prog_id) &&
                    ((p.start_date_time.before(syncedTime) &&
                      syncedTime.getTime < (p.start_date_time.getTime - TimeUnit.MINUTES.toMillis(5) + TimeUnit.MINUTES.toMillis(p.prog_duration.toInt)))
                      || p.start_date_time == syncedTime)
                })
                  .map(p => (p.prog_id, 1))
                  .distinct
                  .toMap

                val translated_genres = trans_progs.flatMap(p => {
                  val genres = if (progsToGenres.contains(p._1)) progsToGenres(p._1) else Array[String]()
                  genres
                }).toList

                predict_result.translatedProgsAvg += trans_progs.keys.size
                //.collect()
                //println("view start=" + view.event_date_time + " prog_id " + view.prog_code +
                //  " translated programs at this time " + trans_progs.length)

                // Find top rated translated program
                //println("Find top rated translated program from array")

                // Group program prediction
                val rated_genres = rated_genres_by_clusters(pOfDay)(current_clust_num)
                var done = false
                var group_prog_position: Int = 1000
                var index = 0
                if (progsToGenres.contains(view.prog_code)) {
                  for (i <- rated_genres.indices; if !done) {
                    if (translated_genres.contains(rated_genres(i)._1)) {
                      if (progsToGenres(view.prog_code).contains(rated_genres(i)._1)) {
                        if (group_prog_position == 1000) { // Recognize first time entrance
                          group_prog_position = index
                          done = true
                        }
                      }
                      index += 1
                      if (index == 20) {
                        done = true
                      }
                    }
                  }
                  predict_result.group_Precision_At1 += (if (group_prog_position == 0) 1 else 0)
                  predict_result.group_Precision_At5 += (if (group_prog_position < 5) 1 else 0)
                  predict_result.group_Precision_At10 += (if (group_prog_position < 10) 1 else 0)
                  predict_result.group_Precision_At15 += (if (group_prog_position < 15) 1 else 0)
                  predict_result.group_Precision_At20 += (if (group_prog_position < 20) 1 else 0)

                  // Individual program prediction
                  done = false
                  var prog_position: Int = 1000
                  index = 0
                  val current_genre_rates = device.genre_rates(pOfDay)._2
                  for (i <- current_genre_rates.indices; if !done) {
                    if (translated_genres.contains(current_genre_rates(i)._1)) {
                      if (progsToGenres(view.prog_code).contains(current_genre_rates(i)._1)) {
                        if (prog_position == 1000) { // Recognize first time entrance
                          prog_position = index
                          done = true
                        }
                      }
                      index += 1
                      if (index == 20) {
                        done = true
                      }
                    }
                  }
                  predict_result.precision_At1 += (if (prog_position == 0) 1 else 0)
                  predict_result.precision_At5 += (if (prog_position < 5) 1 else 0)
                  predict_result.precision_At10 += (if (prog_position < 10) 1 else 0)
                  predict_result.precision_At15 += (if (prog_position < 15) 1 else 0)
                  predict_result.precision_At20 += (if (prog_position < 20) 1 else 0)
                }
              }
            }
            if (predict_result.guesses > 0) {
              val guesses = predict_result.guesses.toDouble
              predict_result.group_Precision_At1 = predict_result.group_Precision_At1 / guesses
              predict_result.group_Precision_At5 = predict_result.group_Precision_At5 / guesses
              predict_result.group_Precision_At10 = predict_result.group_Precision_At10 / guesses
              predict_result.group_Precision_At15 = predict_result.group_Precision_At15 / guesses
              predict_result.group_Precision_At20 = predict_result.group_Precision_At20 / guesses

              predict_result.precision_At1 = predict_result.precision_At1 / guesses
              predict_result.precision_At5 = predict_result.precision_At5 / guesses
              predict_result.precision_At10 = predict_result.precision_At10 / guesses
              predict_result.precision_At15 = predict_result.precision_At15 / guesses
              predict_result.precision_At20 = predict_result.precision_At20 / guesses

              predict_result.translatedProgsAvg = predict_result.translatedProgsAvg / guesses
            }

            val t1 = System.nanoTime()
            //println("Elapsed time: " + TimeUnit.NANOSECONDS.toSeconds(t1 - t0) + "s")

            predict_result
          }

        //sc.parallelize(predict_results.take(20))

        if(FileSystem.get(new URI("s3n://magnet-fwm"), sc.hadoopConfiguration).exists(new Path(results_path)))
        {
          println("Deleting " + results_path)
          FileSystem.get(new URI("s3n://magnet-fwm"), sc.hadoopConfiguration).delete(new Path(results_path), true)
        }

        predict_results
          .map(d => d._2)
          //.coalesce(1)
          .saveAsTextFile(results_path)
      }
      //System.exit(0)
      //</editor-fold>
    }

    println("Done!")

    //</editor-fold>
  }
}

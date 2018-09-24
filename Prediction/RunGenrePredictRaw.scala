package SetTopBoxes.Prediction

import java.io.Serializable
import java.net.URI
import java.text.SimpleDateFormat
import java.util.concurrent.TimeUnit

import SetTopBoxes.Trash.RunGenrePredict.sc
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.compress.GzipCodec
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object RunGenrePredictRaw {
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
                   codes: Seq[String],
                   genres: Seq[String]
                 ) extends Serializable

  case class RatedProg(
                        index: Long,
                        rate: Double
                      ) extends Serializable

  case class Device(
                     device_id: String,
                     views: Iterable[String],
                     genre_rates: Map[Int, (Int, Seq[(Int,Double)])]
                   ) extends Serializable

  class PredictResultAttemp(
                             device_id: String,
                             city: String,
                             found: Int,
                             gr_found: Int,
                             genre_position: Int,
                             gr_genre_position: Int,
                             cluster_num: Int,
                             total_cls_num: Int,
                             part: Int,
                             event_date_time: java.sql.Timestamp,
                             prog_code: String,
                             translated_genres: Int,
                             prog_genres: Seq[Int],
                             suggested_genres_group: Seq[Int],
                             suggested_genres_ind: Seq[Int]
                           ) extends Serializable {
    override def toString: String =
        s"$device_id|$city|$found|$gr_found|$genre_position|$gr_genre_position|" +
        s"$cluster_num|$total_cls_num|$part|$event_date_time|$prog_code|$translated_genres|${prog_genres.mkString(",")}"+
        s"|${suggested_genres_group.mkString(",")}|${suggested_genres_ind.mkString(",")}"
  }

  object PartsOfDay extends Serializable {

    import java.util.{Calendar, Locale}

    private val daynameFormat = new SimpleDateFormat("EEEE", Locale.US)

    // 1 parts - simple cluster over all.
    // 4 parts - 4 parts of day, morning(06-12), noon(12-19), evening(19-02), night(02-06).
    // 5 parts - 5 parts of day, morning(06-12), noon(12-16), evening(16-21), late evening(21-02), night(02-06).
    // 5 parts new - 5 parts of day, morning(06-11), noon(11-14), evening(14-20), late evening(20-02), night(02-06).
    // 8 parts - 4 parts of day ones for weekdays and ones for weekend.
    // 10 parts - 5 parts of day ones for weekdays and ones for weekend.
    // 7 parts - clustering on each of week day separately.
    // 28 parts - 4 parts of day for each week day. 4*7

    //Weekend -> Saturday and Sunday
    def GetPartOfDay(parts: Int, date: java.sql.Timestamp, div5new: Boolean): Int = {
      var partOfDay = 1

      val needDivisionByParts4 = parts == 4 || parts == 8 || parts == 28
      val needDivisionByParts5 = parts == 5 || parts == 10
      val needDivisionWeekdaysWeekends = parts == 2 || parts == 8 || parts == 10
      val needDivisionByDaysOfWeek = parts == 7 || parts == 28

      // Parts of day
      if (needDivisionByParts4 || needDivisionByParts5) {
        val hour = TimeUnit.MILLISECONDS.toHours(date.getTime) % 24
        if (needDivisionByParts4) {
          if (hour >= 6 && hour < 12) {
            partOfDay = 1
          }
          else if (hour >= 12 && hour < 19) {
            partOfDay = 2
          }
          else if (hour >= 19 || hour <= 2) {
            partOfDay = 3
          }
          else {
            partOfDay = 4
          }
        }
        else if (needDivisionByParts5) {
          if (div5new) {
            if (hour >= 6 && hour < 11) {
              partOfDay = 1
            }
            else if (hour >= 11 && hour < 14) {
              partOfDay = 2
            }
            else if (hour >= 14 && hour < 20) {
              partOfDay = 3
            }
            else if (hour >= 20 || hour < 2) {
              partOfDay = 4
            }
            else {
              partOfDay = 5
            }
          }
          else {
            if (hour >= 6 && hour < 12) {
              partOfDay = 1
            }
            else if (hour >= 12 && hour < 16) {
              partOfDay = 2
            }
            else if (hour >= 16 && hour < 21) {
              partOfDay = 3
            }
            else if (hour >= 21 || hour < 2) {
              partOfDay = 4
            }
            else {
              partOfDay = 5
            }
          }
        }
      }

      // Weekends/weekdays
      if (needDivisionWeekdaysWeekends) { // parts == 2 => weekdays or weekends, parts == 8 => 4 part of days for weekdays and weekend
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
            partOfDay += (if (parts == 8) 4 else 5)
          }
        }
      }

      // Days of week
      if (needDivisionByDaysOfWeek) { // 7 days
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

    println("\n" + "#" * 70 + " Run genre prediction " + "#" * 70 + "\n")

    //<editor-fold desc="Init vars and settings region">

    val partsOfDay = 10
    val useDataWithTimeDecay = true
    val timeDecayRate = "_70"
    val predictOnlyOnTuningEvent = false
    val LearnedOnlyFromTuningEvent = false
    val div5new = false
    val learnRegularPredictOnEvents = predictOnlyOnTuningEvent && !LearnedOnlyFromTuningEvent
    val useScaleOf10Genres = true

    println("#" * 50 + " parts of days " + partsOfDay + " " + "#" * 50)
    val format = new SimpleDateFormat("yyyyMMddHHmmss")
    var basePath = "s3n://magnet-fwm/home/TsviKuflik/"
    val notAllowedPRograms = Array[String]("MP_MISSINGPROGRAM", "DVRPGMKEY"
      , "EP000009937449", "SH000191120000", "SH000000010000", "H002786110000", "SH015815970000", "SH000191160000"
      , "SH000191300000", "SH000191680000", "SH001083440000", "SH003469020000", "SH007919190000", "SH008073210000"
      , "SH014947440000", "SH016938330000", "SH018049520000", "SH020632040000", "SP003015230000")

    val programs_path = basePath + "y_programs/programs_genres_groups/part-00000"
    val views_path = basePath + "y_views_with_durations"

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
    println("useDataWithTimeDecay=" + useDataWithTimeDecay)
    println("timeDecayRate=" + timeDecayRate)
    println("predictOnlyOnTuningEvent=" + predictOnlyOnTuningEvent)
    println("LearnedOnlyFromTuningEvent=" + LearnedOnlyFromTuningEvent)
    println("div5new=" + div5new)
    println("useScaleOf10Genres=" + useScaleOf10Genres)

    //</editor-fold>

    //<editor-fold desc="Load cities diff hours and cluster numbers">

    val cities = Map[String, CityData]("Amarillo" -> CityData(5, scala.collection.mutable.Map[Int, Int]()), "Parkersburg" -> CityData(4, scala.collection.mutable.Map[Int, Int]()),
      "Little Rock-Pine Bluff" -> CityData(5, scala.collection.mutable.Map[Int, Int]()), "Seattle-Tacoma" -> CityData(7, scala.collection.mutable.Map[Int, Int]()))

    for (city_name <- cities.keys) {
      val city_name_safe = city_name.replace(" ", "-")
      for (pOfDay <- 1 until (partsOfDay + 1)) {
        val selected_k_path = basePath + "y_clustering_results/5_months_gen" +
          (if (useDataWithTimeDecay) "_with_decay" + timeDecayRate else "") +
          (if (LearnedOnlyFromTuningEvent) "_on_tuning" else "") +
          (if (useScaleOf10Genres) "_scale10" else "") +
          "/" + city_name_safe + "_vect_" + partsOfDay + "/" +
          (if (partsOfDay == 1) "selected_k" else "part" + pOfDay + (if (div5new) "_v2" else "") + "/selected_k") + "/part*"

        val k = sc.textFile(selected_k_path).map(s => s.split(",")(0).toInt).take(1).head
        cities(city_name).cluster_nums += pOfDay -> k
      }
    }

    for (city_name <- cities.keys) {
      println(city_name)
      for (pOfDay <- cities(city_name).cluster_nums.keys) {
        println("part " + pOfDay + " k=" + cities(city_name).cluster_nums(pOfDay))
      }
    }

    //</editor-fold>

    //<editor-fold desc="Programs and translated programs load">

    println("RDD programs section")
    val prograrms = sc.textFile(programs_path)
      .zipWithIndex
      .map { case (s, index) =>
        val sp = s.split("\\|")
        val genres = sp(1)
        Prog(sp.slice(2, sp.length), if (genres == null || genres.isEmpty) null else genres.split(","))
      }
      .collect()

    val GenresArray = sc.textFile(basePath + "y_programs/genres/part*")
      .map(_.split("\\|"))
      .filter(s => !s(0).isEmpty)
      .map(s => s(0))
      .collect()
      .sortBy(s => s)
      .toList

    var scaleOf10GenresList: Map[String, Int] = null
    if(useScaleOf10Genres) {
      scaleOf10GenresList = sc.textFile(basePath + "y_programs/genres/10-genres-maping.txt")
        .map(s => {
          val sp = s.split(",")
          val genreName = sp(1)
          val genreScal10Index = sp(2).toInt
          (genreName, genreScal10Index)
        })
        .collect()
        .toMap
    }

    // Program code to prog index dict
    val programsCodesToGenresIndexArray: Map[String, Seq[Int]] = prograrms
      .flatMap { p =>
        if (p.genres != null) {
          val indecies =  p.genres.map(g => {
            if (useScaleOf10Genres) scaleOf10GenresList(g) else GenresArray.indexOf(g)
          })
          p.codes.map(t => (t, indecies))
        }
        else {
          p.codes.map(t => (t, null))
        }
      }
      .filter(t => t._2 != null)
      .toMap

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
    println("Translated programs fetched")

    //translatedPrograms.take(10).foreach(p=>println(p))

    //</editor-fold>

    for (citykey <- cities.keys) {

      val t0 = System.nanoTime()

      //<editor-fold desc="City vars">

      val city_name = citykey
      val city_name_safe = city_name.replace(" ", "-")

      val base_file_name = "/5_months_gen" +
        (if (useDataWithTimeDecay) "_with_decay" + timeDecayRate else "") +
        (if (LearnedOnlyFromTuningEvent) "_on_tuning" else "") +
        (if (useScaleOf10Genres) "_scale10" else "") +
        "/" + city_name_safe + "_vect_" + partsOfDay

      var clusteredDataPath = ""
      val devices_views_path = basePath + "y_for_prediction/" +
        (if (predictOnlyOnTuningEvent) "views_on_tuning" else "views") +
        "/5_months/" + city_name_safe + "-0601-0608/*.gz"

      val results_path = basePath + "y_predictions/5_months_gen" +
        (if (useDataWithTimeDecay) "_with_decay" + timeDecayRate else "") +
        (if (predictOnlyOnTuningEvent) "_on_tuning" else "") +
        (if (learnRegularPredictOnEvents) "_test" else "") +
        (if (useScaleOf10Genres) "_scale10" else "") +
        "/" + city_name_safe + "_raw_" + partsOfDay + (if (div5new) "_v2" else "")

      println("results_path=" + results_path)

      //</editor-fold>

      //<editor-fold desc="Clustered data load">

      // "cluster","part","device_id","genre_14","genre_16","genre_20","genre_39","genre_49"...
      val vectors_headers = mutable.Map[Int, Map[Int, Int]]() // part -> (vector index -> prog index)

      if (partsOfDay > 1) {
        for (pOfDay <- 1 until (partsOfDay + 1)) {
          val clusters_num = cities(city_name).cluster_nums(pOfDay)
          val path = basePath + "y_clustering_results" + base_file_name + "/part" +
            pOfDay + (if (div5new) "_v2" else "") + "/k_" + clusters_num + "/clustered/*.gz"
          clusteredDataPath += path + (if (pOfDay < partsOfDay) "," else "")

          val sp = sc.textFile(path).first().split(",")
          val headers = sp.slice(3, sp.length).map(s => s.replace("\"", "").replace("genre_", "").toInt)
          vectors_headers(pOfDay) = headers.zipWithIndex.map { case (v, indx) => (indx, v) }.toMap
        }
      }
      else {
        val clusters_num = cities(city_name).cluster_nums(1)
        clusteredDataPath = basePath + "y_clustering_results" + base_file_name + "/k_" + clusters_num + "/clustered/*.gz"

        val sp = sc.textFile(clusteredDataPath).first().split(",")
        val headers = sp.slice(3, sp.length).map(s => s.replace("\"", "").replace("genre_", "").toInt)
        vectors_headers(1) = headers.zipWithIndex.map { case (v, indx) => (indx, v) }.toMap
      }
      println("Vector headers fetched")

      println("clusteredDataPath=" + clusteredDataPath)
      // Clustered data RDD[DataPoint]
      val clusteredData: RDD[DataPoint] = sc.textFile(clusteredDataPath)
        .repartition(1200)
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

      // Map[part,Map[clster_num,Map[genreIndex,rate]]]
      val rated_genres_by_clusters: Map[Int, Map[Int, Seq[(Int, Double)]]] = clusteredData
        .map(p => (p.part, (p.clust_num, p.data)))
        .groupBy(p => p._1)
        .map(v => {
          val part = v._1
          val vector_headers = vectors_headers(v._2.head._1) // part -> vector index -> prog index
          val clust_vec: Map[Int, Seq[(Int, Double)]] = v._2.map(g => g._2)
            .groupBy(c => c._1)
            .map(x => {
              // x._1 cluster number, X._2 vector
              val clust_num = x._1
              // vector that is the sum of all vectors
              val clust_vector = x._2.map(c1 => c1._2)
                .reduce((v1, v2) =>
                  (v2.toArray, v1.toArray).zipped.map(_ + _).toSeq
                )
              // average
              val clust_vector_avg = clust_vector.map(c1 => c1 / x._2.size.toDouble)

              //(clust ,clust_vector_avg)
              // rated genres
              val rated: Seq[(Int, Double)] = clust_vector_avg
                .zipWithIndex
                .map { case (rate, indx) =>
                  val genre_index = vectors_headers(part)(indx)
                  (genre_index, rate)
                }
                .filter(v => v._2 > 0)
                .sortBy(-_._2)

              (clust_num, rated)
            }
            )
          (part, clust_vec)
        }
        ).collect()
        .toMap

      println("rated_programs_by_clusters finished")
      for (key <- rated_genres_by_clusters.keys) {
        println("part " + key + " clust_nums " + rated_genres_by_clusters(key).keys.mkString(","))
      }

      //</editor-fold>

      //<editor-fold desc="Load devices with views and clusters numbers">

      val devicesViews: RDD[(String, Array[String])] = sc.textFile(devices_views_path)
        //.repartition(1200)
        .map(s => {
        val splited = s.split(",").toList
        val device_id = splited.head
        val views = splited.slice(1, s.length)
        //.map(v => {
        //  val sp = v.split("\\|")
        //  val dt = new java.sql.Timestamp(format.parse(sp(0)).getTime)
        //  ViewingData(event_date_time = dt, prog_code = sp(1), duration = sp(2).toInt)
        //})
        (device_id, views.toArray)
      })

      //println("devicesViews:")
      //devicesViews.collect.foreach(v=>println(v._1 + " -> " + v._2.length))

      // create personal program rates.
      //println("clusteredDataDC:")

      val clusteredDataDC: RDD[(String, Map[Int, (Int, Seq[(Int, Double)])])] = clusteredData
        .groupBy(d => d.device_id)
        .map(d => {
          val prog_rate_maps = d._2.map(dp => {
            val prog_rate_map = dp.data.zipWithIndex
              .map { case (rate, indx) =>
                val genre_index = vectors_headers(dp.part)(indx) // vector index -> prog index
                (genre_index, rate)
              }
              .sortBy(v => -(v._2))

            (dp.part, (dp.clust_num, prog_rate_map))
          }).toMap

          (d._1, prog_rate_maps)
        })

      //clusteredDataDC.foreachPartition(iter => println("clusteredDataDC elements in this partition: " + iter.length))

      val devicesWithViewsAndClusters: RDD[Device] = devicesViews.join(clusteredDataDC)
        .map(d => {
          Device(device_id = d._1, views = d._2._1, genre_rates = d._2._2)
        })
      //.repartition(2000)

      //println("devicesWithViewsAndClusters count " + devicesWithViewsAndClusters.count())

      //</editor-fold>

      //<editor-fold desc="Prediction">

      println("Starting prediction " + city_name)

      val predict_results = devicesWithViewsAndClusters
        .map { device =>

          var predictResults: ListBuffer[PredictResultAttemp] = ListBuffer[PredictResultAttemp]()

          //println("#" + index + " device " + device.device_id + " device.views=" + device.views.size)

          val views = device.views.map(v => {
            val sp = v.split("\\|")
            val dt = new java.sql.Timestamp(format.parse(sp(0)).getTime)
            ViewingData(event_date_time = dt, prog_code = sp(1), duration = sp(2).toInt)
          })

          for (view <- views) {

            if (!notAllowedPRograms.contains(view.prog_code) && programsCodesToGenresIndexArray.contains(view.prog_code)) {

              var pOfDay = PartsOfDay.GetPartOfDay(partsOfDay, view.event_date_time, div5new)
              val syncedTime = new java.sql.Timestamp(view.event_date_time.getTime + TimeUnit.HOURS.toMillis(cities(city_name).hours_diff))
              val current_clust_num = device.genre_rates(pOfDay)._1
              val currentProgramGenres: Seq[Int] = if (programsCodesToGenresIndexArray.contains(view.prog_code))
                programsCodesToGenresIndexArray(view.prog_code).toList else Seq[Int]()

              // Find translated programs
              //println("Find translated programs")
              var trans_progs = translatedPrograms.filter(p => {
                !notAllowedPRograms.contains(p.prog_id) &&
                  ((p.start_date_time.before(syncedTime) &&
                    syncedTime.getTime < (p.start_date_time.getTime - TimeUnit.MINUTES.toMillis(5) + TimeUnit.MINUTES.toMillis(p.prog_duration.toInt)))
                    || p.start_date_time == syncedTime)
              })

              val translatedGenres = trans_progs.map(p => {
                  if (programsCodesToGenresIndexArray.contains(p.prog_id)) {
                    programsCodesToGenresIndexArray(p.prog_id).toList
                  }
                  else {
                    null
                  }
                })
                .filter(g => g != null)
                .flatMap(_.map(_.toInt))
                .distinct
                .toList

              //println("view start=" + view.event_date_time + " prog_id " + view.prog_code +
              //  " translated programs at this time " + trans_progs.length)

              // Find top rated translated program
              //println("Find top rated translated program from array")

              // Group program prediction
              val cluster_rated_genres: Seq[(Int, Double)] = rated_genres_by_clusters(pOfDay)(current_clust_num)
              val suggestListSize = if (useScaleOf10Genres) 10 else 20
              var done = false
              var group_genre_position: Int = 1000
              var index = 0

              val ratedGenresContainsThis = cluster_rated_genres.exists(v => currentProgramGenres.contains(v._1))
              if (ratedGenresContainsThis) {
                for (i <- cluster_rated_genres.indices; if !done) {
                  if (translatedGenres.contains(cluster_rated_genres(i)._1)){
                    if (currentProgramGenres.contains(cluster_rated_genres(i)._1)) {
                      if (group_genre_position == 1000) { // Recognize first time entrance
                        group_genre_position = index
                        done = true
                      }
                    }
                    index += 1
                    if (index == (suggestListSize + 1)) {
                      done = true
                    }
                  }
                }
              }
              else {
                group_genre_position = -1
              }

              // Individual program prediction
              done = false
              var prog_position: Int = 1000
              index = 0

              val device_genre_rates:  Seq[(Int, Double)] = device.genre_rates(pOfDay)._2
              val ratedProgramsContainsThis = device_genre_rates.exists(v => currentProgramGenres.contains(v._1))
              if (ratedProgramsContainsThis) {
                for (i <- device_genre_rates.indices; if !done) {
                  if (translatedGenres.contains(device_genre_rates(i)._1)) {
                    if (currentProgramGenres.contains(device_genre_rates(i)._1)) {
                      if (prog_position == 1000) { // Recognize first time entrance
                        prog_position = index
                        done = true
                      }
                    }
                    index += 1
                    if (index == (suggestListSize + 1)) {
                      done = true
                    }
                  }
                }
              }
              else {
                prog_position = -1
              }

              val total_clust_num: Int = cities(city_name).cluster_nums(pOfDay)

              val pred_result = new PredictResultAttemp(
                device.device_id,
                city_name,
                if (ratedProgramsContainsThis) 1 else 0,
                if (ratedGenresContainsThis) 1 else 0,
                prog_position,
                group_genre_position,
                current_clust_num,
                total_clust_num,
                pOfDay,
                syncedTime,
                view.prog_code,
                translatedGenres.length,
                currentProgramGenres,
                cluster_rated_genres.take(suggestListSize).map(_._1),
                device_genre_rates.take(suggestListSize).map(_._1))

              predictResults.append(pred_result)
            }
          }

          //val t1 = System.nanoTime()
          //println("Elapsed time: " + TimeUnit.NANOSECONDS.toSeconds(t1 - t0) + "s")

          predictResults.map(p => p.toString).toArray
        }

      if (FileSystem.get(new URI("s3n://magnet-fwm"), sc.hadoopConfiguration).exists(new Path(results_path))) {
        println("Deleting " + results_path)
        FileSystem.get(new URI("s3n://magnet-fwm"), sc.hadoopConfiguration).delete(new Path(results_path), true)
      }
      //sc.parallelize(predict_results.take(20))
      predict_results
        .flatMap(l => l)
        .saveAsTextFile(results_path, classOf[GzipCodec])

      val t1 = System.nanoTime()
      println("Elapsed time: " + TimeUnit.NANOSECONDS.toMinutes(t1 - t0) + "s")
      //System.exit(0)
      //</editor-fold>
    }

    println("Done!")

    //</editor-fold>
  }
}

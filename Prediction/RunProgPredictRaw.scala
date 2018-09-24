package SetTopBoxes.Prediction

import java.io.Serializable
import java.net.URI
import java.text.SimpleDateFormat
import java.util.concurrent.TimeUnit

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.compress.GzipCodec
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object RunProgPredictRaw {
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
                   index: Long,
                   codes: Seq[String]
                 ) extends Serializable

  case class RatedProg(
                        index: Long,
                        rate: Double
                      ) extends Serializable

  case class Device(
                     device_id: String,
                     views: Iterable[String],
                     prog_rates: Map[Int, (Int, Seq[(String, RatedProg)])]
                   ) extends Serializable

  class PredictResultAttemp(
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

  object PartsOfDay extends Serializable {

    import java.util.Locale
    import java.util.Calendar

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
    def GetPartOfDay(parts: Int, date: java.sql.Timestamp, div5new:Boolean): Int = {
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
          if(div5new){
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
          else{
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

    println("\n" + "#" * 70 + " RunProgPredict Raw " + "#" * 70 + "\n")

    //<editor-fold desc="Init vars and settings region">

    val partsOfDay = 10
    val useDataWithTimeDecay = true
    val timeDecayRate = "_70"
    val predictOnlyOnTuningEvent = false
    val LearnedOnlyFromTuningEvent = false
    val div5new = true
    val learnRegularPredictOnEvents = predictOnlyOnTuningEvent && !LearnedOnlyFromTuningEvent

    println("#" * 50 + " parts of days " + partsOfDay + " " + "#" * 50)
    val format = new SimpleDateFormat("yyyyMMddHHmmss")
    var basePath = "s3n://magnet-fwm/home/TsviKuflik/"
    val notAllowedPRograms = Array[String]("MP_MISSINGPROGRAM", "DVRPGMKEY"
      , "EP000009937449", "SH000191120000", "SH000000010000", "H002786110000", "SH015815970000", "SH000191160000"
      , "SH000191300000", "SH000191680000", "SH001083440000", "SH003469020000", "SH007919190000", "SH008073210000"
      , "SH014947440000", "SH016938330000", "SH018049520000", "SH020632040000", "SP003015230000")

    val programs_path = basePath + "y_programs/programs_groups/part-00000"
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

    //</editor-fold>

    //<editor-fold desc="Load cities diff hours and cluster numbers">

    val cities = Map[String, CityData]("Amarillo" -> CityData(5, scala.collection.mutable.Map[Int, Int]()), "Parkersburg" -> CityData(4, scala.collection.mutable.Map[Int, Int]()),
      "Little Rock-Pine Bluff" -> CityData(5, scala.collection.mutable.Map[Int, Int]()), "Seattle-Tacoma" -> CityData(7, scala.collection.mutable.Map[Int, Int]()))

    for (city_name <- cities.keys) {
      val city_name_safe = city_name.replace(" ", "-")
      for (pOfDay <- 1 until (partsOfDay + 1)) {
        val selected_k_path = basePath + "y_clustering_results/5_months" + (if (useDataWithTimeDecay) "_with_decay" + timeDecayRate else "") +
          (if (LearnedOnlyFromTuningEvent) "_on_tuning" else "" ) + "/" +
          city_name_safe + "_vect_" + partsOfDay + "/" +
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
        //(index, sp(0), sp.slice(1, sp.length)) //(index, name, programs codes)
        Prog(index, sp.slice(1, sp.length))
      }
      .collect()

    // Program code to prog index dict
    val programsCodesToIndex: Map[String, Long] = prograrms
      .flatMap { p => p.codes.map(t => (t, p.index)) } // (program code,program index in programsVector )
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

      val base_file_name = "/5_months" + (if (useDataWithTimeDecay) "_with_decay" + timeDecayRate else "") +
        (if (LearnedOnlyFromTuningEvent) "_on_tuning" else "" ) + "/" + city_name_safe + "_vect_" + partsOfDay
      var clusteredDataPath = ""
      val devices_views_path = basePath + "y_for_prediction/" + (if (predictOnlyOnTuningEvent) "views_on_tuning" else "views") +
        "/5_months/" + city_name_safe + "-0601-0608/*.gz"

      val results_path = basePath + "y_predictions/5_months" + (if (useDataWithTimeDecay) "_with_decay" + timeDecayRate else "") +
        (if (predictOnlyOnTuningEvent) "_on_tuning" else "") + (if (learnRegularPredictOnEvents) "_test" else "") +
        "/" + city_name_safe + "_raw_" + partsOfDay + (if (div5new) "_v2" else "")

      println("results_path=" + results_path)

      //</editor-fold>

      //<editor-fold desc="Clustered data load">

      // "cluster","part","device_id","prog_14","prog_16","prog_20","prog_39","prog_49"...
      val vectors_headers = mutable.Map[Int, Map[Int, Int]]() // part -> (vector index -> prog index)

      if (partsOfDay > 1) {
        for (pOfDay <- 1 until (partsOfDay + 1)) {
          val clusters_num = cities(city_name).cluster_nums(pOfDay)
          val path = basePath + "y_clustering_results" + base_file_name + "/part" +
            pOfDay + (if (div5new) "_v2" else "") + "/k_" + clusters_num + "/clustered/*.gz"
          clusteredDataPath += path + (if (pOfDay < partsOfDay) "," else "")

          val sp = sc.textFile(path).first().split(",")
          val headers = sp.slice(3, sp.length).map(s => s.replace("\"", "").replace("prog_", "").toInt)
          vectors_headers(pOfDay) = headers.zipWithIndex.map { case (v, indx) => (indx, v) }.toMap
        }
      }
      else {
        val clusters_num = cities(city_name).cluster_nums(1)
        clusteredDataPath = basePath + "y_clustering_results" + base_file_name + "/k_" + clusters_num + "/clustered/*.gz"

        val sp = sc.textFile(clusteredDataPath).first().split(",")
        val headers = sp.slice(3, sp.length).map(s => s.replace("\"", "").replace("prog_", "").toInt)
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

      // Map[part,Map[clster_num,List[program code,rate]]]
      val rated_programs_by_clusters = clusteredData
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
                .flatMap { case (rate, indx) =>
                  val prog_index = vector_headers(indx) // vector index -> prog index
                  val prog = prograrms(prog_index)
                  prog.codes.map(prog_code => (prog_code, RatedProg(prog_index, rate)))
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

      println("rated_programs_by_clusters finished")
      for (key <- rated_programs_by_clusters.keys) {
        println("part " + key + " clust_nums " + rated_programs_by_clusters(key).keys.mkString(","))
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

      val clusteredDataDC: RDD[(String, Map[Int, (Int, Seq[(String, RatedProg)])])] = clusteredData
        .groupBy(d => d.device_id)
        .map(d => {
          val prog_rate_maps = d._2.map(dp => {
            val prog_rate_map = dp.data.zipWithIndex
              .flatMap { case (rate, indx) =>
                val prog_index = vectors_headers(dp.part)(indx) // vector index -> prog index
              val prog = prograrms(prog_index)
                prog.codes.map(c => (c, RatedProg(prog_index, rate)))
              }
              .sortBy(v => -v._2.rate)

            (dp.part, (dp.clust_num, prog_rate_map))
          }).toMap

          (d._1, prog_rate_maps)
        })

      //clusteredDataDC.foreachPartition(iter => println("clusteredDataDC elements in this partition: " + iter.length))

      val devicesWithViewsAndClusters: RDD[Device] = devicesViews.join(clusteredDataDC)
        .map(d => {
          Device(device_id = d._1, views = d._2._1, prog_rates = d._2._2)
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
            if (!notAllowedPRograms.contains(view.prog_code)) {
              var pOfDay = PartsOfDay.GetPartOfDay(partsOfDay, view.event_date_time, div5new)
              val syncedTime = new java.sql.Timestamp(view.event_date_time.getTime + TimeUnit.HOURS.toMillis(cities(city_name).hours_diff))
              val current_clust_num = device.prog_rates(pOfDay)._1

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

              //println("view start=" + view.event_date_time + " prog_id " + view.prog_code +
              //  " translated programs at this time " + trans_progs.length)

              // Find top rated translated program
              //println("Find top rated translated program from array")

              // Group program prediction
              val rated_programs: Seq[(String, RatedProg)] = rated_programs_by_clusters(pOfDay)(current_clust_num)
              var done = false
              var group_prog_position: Int = 1000
              var index = 0

              val ratedGroupProgramsContainsThis = rated_programs.exists(v => v._1 == view.prog_code)
              if (ratedGroupProgramsContainsThis) {
                for (i <- rated_programs.indices; if !done) {
                  if (trans_progs.contains(rated_programs(i)._1)) {
                    if (rated_programs(i)._1 == view.prog_code || rated_programs(i)._2.index == programsCodesToIndex(view.prog_code)) {
                      if (group_prog_position == 1000) { // Recognize first time entrance
                        group_prog_position = index
                        done = true
                      }
                    }
                    index += 1
                    if (index == 50) {
                      done = true
                    }
                  }
                }
              }
              else {
                group_prog_position = -1
              }

              // Individual program prediction
              done = false
              var prog_position: Int = 1000
              index = 0
              val current_prog_rates: Seq[(String, RatedProg)] = device.prog_rates(pOfDay)._2
              val ratedProgramsContainsThis = current_prog_rates.exists(v => v._1 == view.prog_code)
              if (ratedProgramsContainsThis) {
                for (i <- current_prog_rates.indices; if !done) {
                  if (trans_progs.contains(current_prog_rates(i)._1)) {
                    if (current_prog_rates(i)._1 == view.prog_code || current_prog_rates(i)._2.index == programsCodesToIndex(view.prog_code)) {
                      if (prog_position == 1000) { // Recognize first time entrance
                        prog_position = index
                        done = true
                      }
                    }
                    index += 1
                    if (index == 50) {
                      done = true
                    }
                  }
                }
              }
              else {
                prog_position = -1
              }

              val total_clust_num: Int = cities(city_name).cluster_nums(pOfDay)

              val pred_result = new PredictResultAttemp(device.device_id, city_name,
                if (ratedProgramsContainsThis) 1 else 0,
                if (ratedGroupProgramsContainsThis) 1 else 0,
                prog_position, group_prog_position,
                current_clust_num, total_clust_num, pOfDay, syncedTime, view.prog_code, trans_progs.keys.size)
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

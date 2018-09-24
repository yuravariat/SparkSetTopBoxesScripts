package SetTopBoxes.Prediction

import java.io.Serializable
import java.net.URI
import java.text.SimpleDateFormat
import java.util.Calendar
import java.util.concurrent.TimeUnit

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.compress.GzipCodec
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object RunProgPredictRawBaseLine {
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

  case class Device(
                     device_id: String,
                     views: Iterable[String]
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
                             translated_progs_num: Int,
                             additional_info: String,
                             suggested_prog_ind: Seq[Long],
                             suggested_prog_group: Seq[Long]
                           ) extends Serializable {
    override def toString: String =
      s"$device_id|$city|$found|$gr_found|$program_position|$gr_program_position|" +
      s"$cluster_num|$total_cls_num|$part|$event_date_time|$prog_code|$translated_progs_num|$additional_info"+
      s"${suggested_prog_ind.mkString(",")}|${suggested_prog_group.mkString(",")}"
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

    println("\n" + "#" * 70 + " Run Prog Predict BaseLine " + "#" * 70 + "\n")

    //<editor-fold desc="Init vars and settings region">

    val format = new SimpleDateFormat("yyyyMMddHHmmss")
    val dayHourMinutesFormat = new SimpleDateFormat("yyyyMMddHHmm")
    val rated_times_interval = 5
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

    //</editor-fold>

    val cities = Map[String, CityData]("Amarillo" -> CityData(5, scala.collection.mutable.Map[Int, Int]()), "Parkersburg" -> CityData(4, scala.collection.mutable.Map[Int, Int]()),
      "Little Rock-Pine Bluff" -> CityData(5, scala.collection.mutable.Map[Int, Int]()), "Seattle-Tacoma" -> CityData(7, scala.collection.mutable.Map[Int, Int]()))

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

      //<editor-fold desc="City vars">

      val city_name = citykey
      val city_name_safe = city_name.replace(" ", "-")
      val devices_views_path = basePath + "y_for_prediction/views/5_months/" + city_name_safe + "-0601-0608/*.gz"
      val results_path = basePath + "y_predictions/5_months/base_line/progs/" + city_name_safe
      println("results_path=" + results_path)

      //</editor-fold>

      //<editor-fold desc="Load devices with views">

      val devicesViews = sc.textFile(devices_views_path)
        //.filter(s=>s.startsWith("17719-000000df4dd6"))
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
        Device(device_id, views)
      })

      //</editor-fold>

      //<editor-fold desc="Watched rated programs">

      val rated_progs_path = basePath + "y_for_prediction/views_for_base/progs/" + city_name_safe + "-0601-0608"
      val rated_progs_by_views_map = sc.textFile(rated_progs_path)
        .map(s => {
          val sp = s.split("\\|")
          val timeKey = sp(0)
          val r_progs = sp.slice(1, sp.length).map(p => {
            val psp = p.replace(")","").replace("(","").split(",")
            (psp(0), psp(1).toDouble)
          })
            .filter(_._2 > 0.001)

          (timeKey, r_progs)
        })
        .collect()
        .toMap

      //</editor-fold>

      //<editor-fold desc="Prediction">

      println("Starting prediction " + city_name)

      val predict_results = devicesViews
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

              val syncedTime = new java.sql.Timestamp(view.event_date_time.getTime + TimeUnit.HOURS.toMillis(cities(city_name).hours_diff))
              var additional_info = ""

              // Find translated programs
              //println("Find translated programs")
              var trans_progs = translatedPrograms.filter(p => {
                !notAllowedPRograms.contains(p.prog_id) &&
                  ((p.start_date_time.before(syncedTime) &&
                    syncedTime.getTime < (p.start_date_time.getTime - TimeUnit.MINUTES.toMillis(5) + TimeUnit.MINUTES.toMillis(p.prog_duration.toInt)))
                    || p.start_date_time == syncedTime)
              })
                .map(p => (programsCodesToIndex(p.prog_id), p.prog_id))
                .distinct
                .toMap

              // Find rated progs by views Key
              var ratedProgsSearchKey = ""
              val cal = Calendar.getInstance
              cal.setTime(syncedTime)
              val minutes = cal.get(Calendar.MINUTE)
              if ((minutes % rated_times_interval) != 0) {
                if ((minutes % rated_times_interval) < (rated_times_interval / 2)) {
                  ratedProgsSearchKey = dayHourMinutesFormat.format(new java.sql.Timestamp(
                    syncedTime.getTime - TimeUnit.MINUTES.toMillis(minutes % rated_times_interval)))
                }
                else {
                  ratedProgsSearchKey = dayHourMinutesFormat.format(new java.sql.Timestamp(
                    syncedTime.getTime + TimeUnit.MINUTES.toMillis(rated_times_interval - minutes % rated_times_interval)))
                }
              }
              else{
                ratedProgsSearchKey = dayHourMinutesFormat.format(syncedTime)
              }

              additional_info += "{ratedProgsSearchKey:" + ratedProgsSearchKey

              val suggestListRanomApproach: ListBuffer[Long] = ListBuffer[Long]()
              var suggestListMajorityApproach: ListBuffer[Long] = ListBuffer[Long]()
              val r = new scala.util.Random()

              // Select random program with weights and set suggestListMajorityApproach
              if (rated_progs_by_views_map.contains(ratedProgsSearchKey)) {
                val translatedProgsIds = trans_progs.values.toList
                val weightedProgs = rated_progs_by_views_map(ratedProgsSearchKey).filter(p => translatedProgsIds.contains(p._1))

                additional_info += ",weighted:[" + weightedProgs.take(5).mkString(",") + "],weighted_length:" + weightedProgs.length

                suggestListMajorityApproach = weightedProgs.map(p=>programsCodesToIndex(p._1)).take(20).toList.to[ListBuffer]

                if (weightedProgs.length > 0) {

                  val weightedProgsMutable = weightedProgs.to[ListBuffer]

                  for (k <- 0 until math.min(20, weightedProgs.length)) {

                    val sumOfWeights = weightedProgsMutable.map(c => c._2).sum
                    val randomVal = r.nextDouble * sumOfWeights
                    var aggSum = 0.0
                    var done = false
                    for (i <- weightedProgsMutable.indices; if !done) {
                      aggSum += weightedProgsMutable(i)._2
                      if (aggSum >= randomVal) {
                        val index = programsCodesToIndex(weightedProgsMutable(i)._1)
                        suggestListRanomApproach.append(index)
                        weightedProgsMutable.remove(i)
                        done = true
                      }
                    }
                  }
                }
              }

              //<editor-fold desc="Complete suggestion list">

              // Complete suggestion list with pure random
              if (suggestListRanomApproach.length < 20 && trans_progs.size > 20) {
                // Pure random

                val notSuggestedProgs = trans_progs.keys.filter(t => !suggestListRanomApproach.contains(t)).to[ListBuffer]
                additional_info += ",completed_random_ran:" + (20 - suggestListRanomApproach.length)

                val notSuggestedLength = notSuggestedProgs.length
                for(k <- 0 until math.min(notSuggestedLength,(20 - suggestListRanomApproach.length))){
                  val randomVal = (r.nextDouble() * notSuggestedProgs.length).toInt
                  val index = notSuggestedProgs(randomVal)
                  suggestListRanomApproach.append(index)
                  notSuggestedProgs.remove(randomVal)
                }
              }

              // Complete suggestion list with pure random
              if (suggestListMajorityApproach.length < 20 && trans_progs.size > 20) {
                // Pure random

                val notSuggestedProgs = trans_progs.keys.filter(t => !suggestListMajorityApproach.contains(t)).to[ListBuffer]
                additional_info += ",completed_random_ran:" + (20 - suggestListMajorityApproach.length)

                val notSuggestedLength = notSuggestedProgs.length
                for(k <- 0 until math.min(notSuggestedLength,(20 - suggestListMajorityApproach.length))){
                  val randomVal = (r.nextDouble() * notSuggestedProgs.length).toInt
                  val index = notSuggestedProgs(randomVal)
                  suggestListMajorityApproach.append(index)
                  notSuggestedProgs.remove(randomVal)
                }
              }

              //</editor-fold>

              additional_info += ",suggestListRandom:[" + suggestListRanomApproach.take(5).mkString(",") + "]"
              additional_info += ",suggestListMajority:[" + suggestListMajorityApproach.take(5).mkString(",") + "]"

              val currentProgramIndex = programsCodesToIndex(view.prog_code)

              var done = false
              var prog_position_random: Int = 1000
              var index = 0

              // Try to find current watched program in suggestion list
              // Random Approach
              var foundInSuggestionRandomList = false
              if (suggestListRanomApproach.nonEmpty) {
                for (i <- suggestListRanomApproach.indices; if !done) {
                  if (suggestListRanomApproach(i) == currentProgramIndex) { // Found
                    if (prog_position_random == 1000) {
                      prog_position_random = index
                      foundInSuggestionRandomList = true
                      done = true
                    }
                  }
                  index += 1
                }
              }
              else {
                prog_position_random = -1
              }

              done = false
              var prog_position_majority: Int = 1000
              index = 0

              // Try to find current watched program in suggestion list
              // Majority Approach
              var foundInSuggestionMajorityList = false
              if (suggestListMajorityApproach.nonEmpty) {
                for (i <- suggestListMajorityApproach.indices; if !done) {
                  if (suggestListMajorityApproach(i) == currentProgramIndex) { // Found
                    if (prog_position_majority == 1000) {
                      prog_position_majority = index
                      foundInSuggestionMajorityList = true
                      done = true
                    }
                  }
                  index += 1
                }
              }
              else {
                prog_position_majority = -1
              }

              val pred_result = new PredictResultAttemp(
                device_id = device.device_id,
                city = city_name,
                found = if (foundInSuggestionRandomList) 1 else 0,
                gr_found = if (foundInSuggestionMajorityList) 1 else 0,
                program_position = prog_position_random,
                gr_program_position = prog_position_majority,
                cluster_num = 0,
                total_cls_num = currentProgramIndex.toInt,
                part = 1,
                event_date_time = syncedTime,
                prog_code = view.prog_code,
                translated_progs_num = trans_progs.keys.size,
                additional_info = additional_info,
                suggested_prog_ind = suggestListRanomApproach.take(20),
                suggested_prog_group = suggestListMajorityApproach.take(20)
              )

              // Individual - Random method
              // Group - By maority method

              predictResults.append(pred_result)
            }
          }

          predictResults.map(p => p.toString).toArray
        }

      if (FileSystem.get(new URI("s3n://magnet-fwm"), sc.hadoopConfiguration).exists(new Path(results_path))) {
        println("Deleting " + results_path)
        FileSystem.get(new URI("s3n://magnet-fwm"), sc.hadoopConfiguration).delete(new Path(results_path), true)
      }

      predict_results
        .flatMap(l => l)
        .saveAsTextFile(results_path, classOf[GzipCodec])

      //System.exit(0)
      //</editor-fold>
    }

    println("Done!")

    //</editor-fold>
  }
}

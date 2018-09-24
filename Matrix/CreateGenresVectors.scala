package SetTopBoxes.Matrix

import java.net.URI
import java.text.SimpleDateFormat
import java.util.concurrent.TimeUnit

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.compress.GzipCodec
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions.udf

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
  * Created by yurav on 19/04/2017.
  */
object CreateGenresVectors {

  val spark: SparkSession = SparkSession
    .builder()
    .master("local")
    .appName("Scala Spark SQL")
    .config("spark.sql.warehouse.dir", "file:///tmp/spark-warehouse")
    .getOrCreate()

  import spark.implicits._

  //<editor-fold desc="Classes">

  case class ViewingData(
                          device_id_unique: String,
                          event_date_time: java.sql.Timestamp,
                          station_num: String,
                          prog_code: String,
                          var duration: Int,
                          prog_duration: Int
                        ) extends Serializable

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

  object SpecailDates extends Serializable {

    import java.text.SimpleDateFormat
    import org.apache.commons.lang.time.DateUtils
    import java.util.Locale
    import java.util.Calendar

    private val format = new SimpleDateFormat("dd/MM/yyyy")

    private val holidays: List[java.util.Date] = List[java.util.Date](
      format.parse("01/01/2015"), // 1/1/2015 - New Year
      format.parse("19/01/2015"), // 19/1/2015 - Martin Luther King Day
      format.parse("16/02/2015"), // 16/2/2015 - Presidents day
      format.parse("25/05/2015") // 25/5/2015 - Memorial day
      //format.parse("03/07/2015"), format.parse("04/07/2015"), // 3/7/2015-4/7/2015 - Independence day
      //format.parse("07/09/2015"), // 7/9/2015 - Labor day
      //format.parse("11/09/2015"), // 11/9/2015 - September 11 attack
      //format.parse("12/10/2015"), // 12/10/2015 - Columbus day
      //format.parse("11/11/2015"), // 11/11/2015 - Veterans day
      //format.parse("26/11/2015") // 26/11/2015 - Thanksgiving day
    )
    private val events: List[java.util.Date] = List[java.util.Date](
      //format.parse("14/01/2015"), // 14/1/2015 - A collision between a train and a prison transport bus near Penwell, Texas.
      format.parse("26/01/2015"), format.parse("27/01/2015") // 26-27/1/2015 - A blizzard hits the Northeast shutting down major cities including New York City and Boston, with up to 60 million people affected.
      //format.parse("03/02/2015"), // 3/2/2015 - A collision between a commuter train and a passenger vehicle kills six in Valhalla, New York.
      //format.parse("10/02/2015"), // 10/2/2015 - Chapel Hill shooting. Craig Stephen Hicks killed a Muslim family of three in Chapel Hill, North Carolina.
      //format.parse("26/02/2015"), // 26/2/2015 - A gunman kills seven people then himself in a series of shootings in Tyrone, Missouri.
      //format.parse("04/04/2015"), // 4/4/2015 -  Walter Scott, an unarmed man, is shot and killed by a police officer in North Charleston.
      //format.parse("25/04/2015"), format.parse("26/04/2015"), format.parse("27/04/2015"), format.parse("28/04/2015"),
      //format.parse("29/04/2015"), format.parse("30/04/2015"), format.parse("01/05/2015"),
      //format.parse("02/05/2015"), format.parse("03/05/2015"), // 25/4/2015 - 3/5/2015 - Protests in Baltimore, Maryland from Baltimore City Hall to the Inner Harbor against the April 19 death of Freddie Gray in police custody soon turn violent, with extensive property damage
      //format.parse("03/05/2015"), // 3/5/2015 - Two suspected Islamist gunmen attack the Curtis Culwell Center in the city of Garland, Texas.
      //format.parse("12/05/2015"), // 12/5/2015 - An Amtrak train derails in the Philadelphia neighborhood for Port Richmond, causing cars to roll over and killing at least 8 people and injuring over 200.
      //format.parse("17/05/2015"), // 17/5/2015 - A shootout erupts between rival biker gangs in a Twin Peaks restaurant in Waco, Texas, leaving nine dead.
      //format.parse("20/05/2015"), // 20/5/2015 - David Letterman broadcasts the last episode of his 22-year run as host of The Late Show on CBS, drawing a record audience.
      //format.parse("23/05/2015"), format.parse("24/05/2015"), format.parse("25/05/2015"), // 23-25/5/2015 - * Historic flash flooding levels occur in Texas and Oklahoma in a prolonged outbreak of floods and tornadoes, leaving at least 17 people dead and at least 40 others missing.
      //format.parse("03/06/2015"), format.parse("06/06/2015"), format.parse("08/06/2015"), format.parse("10/06/2015"),
      //format.parse("13/06/2015"), format.parse("15/06/2015"), // [3,6,8,10,13,15]/6/2015 - NHL, 2015 Stanley Cup Finals.
      //format.parse("17/06/2015"), // 17/6/2015 - * Nine people are shot and killed during a prayer service at Emanuel African Methodist Episcopal Church, a historically black church
      //format.parse("05/07/2015"), // 5/7/2015 -  2015 FIFA Women's World Cup
      //format.parse("16/07/2015"), // 16/7/2015 - A gunman attacks two military installations in Chattanooga, Tennessee. Five U.S. Marines are killed and two others are injured.
      //format.parse("17/07/2015"), format.parse("18/07/2015"), format.parse("19/07/2015"), format.parse("20/07/2015"),
      //format.parse("21/07/2015"), // 17-21/7/2015 - The Cajon Pass wildfire spreads across 4,250 acres (1,720 ha) in the Mojave Desert near the towns of Victorville and Hesperia, north of San Bernardino and south of Bakersfield in the state of California
      //format.parse("23/07/2015"), // 23/7/2015 - A gunman opens fire at a movie theater in Lafayette, Louisiana, killing two people and injuring nine others before committing suicide.
      //format.parse("26/08/2015"), // 26/8/2015 - News reporter Alison Parker and camera operator Adam Ward are shot and killed on live television during an interview in Moneta, Virginia.
      //format.parse("12/09/2015"), format.parse("13/09/2015"), // 12-13/9/2015 - The Valley wildfire claims at least three lives in Lake County, California with thousands of people forced to evacuate.
      //format.parse("14/09/2015"), // 14/9/2015 - 2015 Utah floods, by Hurricane Linda.
      //format.parse("20/09/2015"), // 20/9/2015 - The 67th Primetime Emmy Awards are held at the Microsoft Theater in Los Angeles, California.
      //format.parse("01/10/2015"), // 1/10/2015 - 26-year-old Christopher Harper-Mercer opens fire at Umpqua Community College in Roseburg, Oregon. killing 9 people and injuring 9 others.
      //format.parse("01/10/2015"), // 1/10/2015 - The SS El Faro, a cargo ship, sinks off the Bahamas after leaving Jacksonville, Florida two days prior, headed to Puerto Rico.
      //format.parse("25/10/2015"), // 25/10/2015 - A drunk driver plows into the Oklahoma State Homecoming parade in Stillwater, Oklahoma, killing four people and injuring 34.
      //format.parse("27/10/2015"), // 27/10/2015-1/11/2015 - World Series, 111th edition of Major League Baseball's championship series.
      //format.parse("22/11/2015"), // 22/11/2015 - The 2015 New Orleans shooting took place at Bunny Friend playground in the Ninth Ward of New Orleans, Louisiana
      //format.parse("27/11/2015"), // 27/11/2015 - A gunman opens fire at a Planned Parenthood clinic in Colorado Springs, Colorado, killing 3, including a police officer, and injuring 9.
      //format.parse("02/12/2015"), // 2/12/2015 - 2015 San Bernardino attack: 14 people are killed in a terrorist attack at a facility for the mentally disabled in San Bernardino, California.
      //format.parse("15/12/2015") // 15/12/2015 - 2015 Los Angeles Unified School District closure: The Los Angeles Unified School District received a credible terrorism threat causing the temporary closure of all Los Angeles Unified Schools.
    )

    def GetDates(): List[java.util.Date] = {
      holidays ::: events
    }

    def GetHolidays(): List[java.util.Date] = {
      holidays
    }

    def GetEvents(): List[java.util.Date] = {
      events
    }

    def IsInDates(date: java.sql.Timestamp): Boolean = {
      if (date == null) {
        return false
      }
      IsInHolidays(date) || IsInEvents(date)
    }

    def IsInHolidays(date: java.sql.Timestamp): Boolean = {
      if (date == null) {
        return false
      }
      for (d <- holidays) {
        if (DateUtils.isSameDay(d, date)) {
          return true
        }
      }
      false
    }

    def IsInEvents(date: java.sql.Timestamp): Boolean = {
      if (date == null) {
        return false
      }
      for (d <- events) {
        if (DateUtils.isSameDay(d, date)) {
          return true
        }
      }
      false
    }
  }

  //</editor-fold>

  /** Main function */
  def main(args: Array[String]): Unit = {
    @transient val conf: SparkConf = new SparkConf().setMaster("local").setAppName("Devices")
    @transient val sc: SparkContext = new SparkContext(conf)

    //<editor-fold desc="Run area">

    println("\n" + "#" * 70 + " Create Genre Vectors " + "#" * 70 + "\n")

    val base_path = "s3n://magnet-fwm/home/TsviKuflik/"
    val partsOfDay = 1
    val applyTimeDecayFunction = false
    val timeDecayRate = 20.0
    val OnlyOnTuningEvent = false
    val div5new = false
    val useScaleOf10Genres = false

    val cities = List[String]("Amarillo", "Parkersburg", "Little Rock-Pine Bluff", "Seattle-Tacoma")
    var devices_views_path = List(base_path + "y_views_with_durations/*.gz")
    if (OnlyOnTuningEvent) {
      devices_views_path = List(
        "s3n://magnet-fwm/home/TsviKuflik/y_for_matrix/views_on_tuning/5_months/Amarillo-0101-0601/*.gz",
        "s3n://magnet-fwm/home/TsviKuflik/y_for_matrix/views_on_tuning/5_months/Parkersburg-0101-0601/*.gz",
        "s3n://magnet-fwm/home/TsviKuflik/y_for_matrix/views_on_tuning/5_months/Little-Rock-Pine-Bluff-0101-0601/*.gz",
        "s3n://magnet-fwm/home/TsviKuflik/y_for_matrix/views_on_tuning/5_months/Seattle-Tacoma-0101-0601/*.gz")
    }
    val format = new SimpleDateFormat("yyyyMMddHHmmss")
    // 1-5, 5 months
    //val startDate = 20150000
    val limitDateForLearn = 20150601
    //val limitDateForDevicesPick = 20150701
    //val minWatchDays = 120
    //val minTotalViews = 2000
    println("#" * 50 + " parts of days " + partsOfDay + " " + "#" * 50)
    val startDate = new java.sql.Timestamp(format.parse("20150101000000").getTime)
    val limitDatePickViews = new java.sql.Timestamp(format.parse(limitDateForLearn.toString + "000000").getTime)
    val months = 5
    val chosen_devices_path = base_path + "y_devices/" + months + "_months/"

    println("cities=" + cities.mkString(","))
    println("limitDateTimeStamp=" + limitDatePickViews)
    println("applyTimeDecayFunction=" + applyTimeDecayFunction + " rate=" + timeDecayRate)
    println("OnlyOnTuningEvent=" + OnlyOnTuningEvent)
    println("div5new=" + div5new)
    println("useScaleOf10Genres=" + useScaleOf10Genres)

    val GenresArray = sc.textFile(base_path + "y_programs/genres/part*")
      .map(_.split("\\|"))
      .filter(s => !s(0).isEmpty)
      .map(s => s(0))
      .collect()
      .sortBy(s => s)
      .toList

    println("genres=" + GenresArray.mkString(","))

    var scaleOf10GenresList: Map[String, Int] = null
    if(useScaleOf10Genres) {
      scaleOf10GenresList = sc.textFile(base_path + "y_programs/genres/10-genres-maping.txt")
        .map(s => {
          val sp = s.split(",")
          val genreName = sp(1)
          val genreScal10Index = sp(2).toInt
          (genreName, genreScal10Index)
        })
        .collect()
        .toMap
      println("scale 10 genres=" + scaleOf10GenresList.map(g=>g._1 + "=>" + g._2).mkString(","))
    }

    //<editor-fold desc="Create programs map">
    println("Creating programs map")
    val notAllowedPrograms = List("MP_MISSINGPROGRAM", "DVRPGMKEY"
      , "EP000009937449", "SH000191120000", "SH000000010000", "H002786110000", "SH015815970000", "SH000191160000"
      , "SH000191300000", "SH000191680000", "SH001083440000", "SH003469020000", "SH007919190000", "SH008073210000"
      , "SH014947440000", "SH016938330000", "SH018049520000", "SH020632040000", "SP003015230000")

    val programs = sc.textFile(base_path + "y_programs/programs_genres_groups/part*")
      .map(s => {
        val sp = s.split("\\|")
        val prog_name = sp(0)
        val genres = sp(1)
        val ids = ListBuffer[String]()
        for (i <- 2 until sp.length) {
          ids.append(sp(i))
        }
        (prog_name, genres, ids)
      })
      .sortBy(_._1)

    val progsToGenresArray = programs
      .filter(p => {
        p._2.!=(null) && !p._2.isEmpty
      })
      .flatMap({ p =>
        p._3.map(id => (id, p._2.split(",")))
      })
      .collect()
      .toMap

    println("Creating programs map end")

    //</editor-fold>

    def IsInSpecialDates = udf((date: java.sql.Timestamp) => {
      SpecailDates.IsInDates(date)
    })

    //<editor-fold desc="Create genres matrix for each city">
    println("Create genres matrix for each city")
    for (cityIndx <- cities.indices) {

      var city_name = cities(cityIndx).replace(" ", "-")
      val city_devices_path = chosen_devices_path + city_name + "/part-00000*"
      val deviceForThisCity = sc.textFile(city_devices_path).map(c => c.trim).collect()
      println("Working on " + city_name + " " + deviceForThisCity.length)

      val devicesRates = spark.read
        .format("com.databricks.spark.csv")
        .load(devices_views_path: _*)
        .filter($"_c0" isin (deviceForThisCity: _*))
        .select(
          $"_c0", // device_id
          $"_c1".cast("timestamp"), // date
          $"_c2", // channel number
          $"_c3", // program code
          $"_c4".cast("int"), // duration
          $"_c5".cast("int") // prog_duration
        )
        .withColumn("in_special_dates", IsInSpecialDates($"_c1"))
        .where($"_c1" < limitDatePickViews && $"in_special_dates" === false && $"_c3" =!= "MP_MISSINGPROGRAM" && $"_c3" =!= "DVRPGMKEY" && $"_c4" > 5)
        .rdd
        .map(r => (r.getString(0), ViewingData(r.getString(0), r.getTimestamp(1), r.getString(2), r.getString(3), r.getInt(4), r.getInt(5))))
        .groupBy(r => r._1)
        .map(f = d => {
          val device_id = d._1
          val views = d._2.map(d => d._2).filter(p => !notAllowedPrograms.contains(p.prog_code)).toList

          val rates: Map[Int, mutable.Map[Long, Double]] = (1 until (partsOfDay + 1)).map(p => (p, mutable.Map[Long, Double]())).toMap

          views.foreach(v => {
            val genres = progsToGenresArray.get(v.prog_code)
            if (genres.isDefined) {
              var partOfDay = PartsOfDay.GetPartOfDay(partsOfDay, v.event_date_time, div5new)
              genres.get.foreach(g => {
                val g_index = if (useScaleOf10Genres) scaleOf10GenresList(g) else GenresArray.indexOf(g)
                var rate = 1.0
                if (applyTimeDecayFunction) {
                  // apply time-decay function
                  val daysDiff = TimeUnit.MILLISECONDS.toDays(limitDatePickViews.getTime - v.event_date_time.getTime)
                  rate = Math.exp(-daysDiff / timeDecayRate)
                }
                if (rates(partOfDay).contains(g_index)) {
                  rates(partOfDay)(g_index) += rate
                }
                else {
                  rates(partOfDay)(g_index) = rate
                }
              })
            }
          })

          // normalize f=> normalized = value / max;
          // full version (but min in our case is always 0), f=> normalized = (value - min) / (max - min);
          for (k <- rates.keys) {
            for (r <- rates(k).keys) {
              val max = rates(k).values.seq.max
              rates(k)(r) = ((rates(k)(r) / max) * 100).toInt
            }
          }

          // The result is (device_id, Map - > (partOfDay ->  Map(prog index-> rate)))
          (device_id, rates)

        })
        .cache()

      for (pOfDay <- 1 until (partsOfDay + 1)) {

        println("Working on part " + pOfDay)
        val resultsPath = "s3n://magnet-fwm/home/TsviKuflik/y_for_clustering/" + months + "_months_gen" +
          (if (applyTimeDecayFunction) "_with_decay_" + timeDecayRate.toInt else "") +
          (if (OnlyOnTuningEvent) "_on_tuning" else "") +
          (if (useScaleOf10Genres) "_scale10" else "") + "/" +
          city_name + "_vect_" + (if (partsOfDay == 1) partsOfDay else partsOfDay + "/part" + pOfDay) + (if (div5new) "_v2" else "")

        println("Output folder: " + resultsPath)

        // Programs indexes to include
        val genre_indexes_to_include = devicesRates
          .flatMap(v => v._2(pOfDay).keys)
          .distinct
          .collect
          .toList
          .sortBy(r => r)

        // Add headers to create csv format.
        val headers: ListBuffer[String] = ListBuffer[String]("\"device_id\"")
        for (i <- genre_indexes_to_include) {
          headers.append("\"genre_" + i + "\"")
        }
        val headerStr: String = headers.mkString(",")

        // headers have to be first so we adding them key 0 and for data key 1. This is the purpose of sortByKey.
        val header = sc.parallelize(List((0, headerStr)))
        val devicesWV = devicesRates.map(p => {
          val deviceVector: Array[Int] = Array.fill[Int](genre_indexes_to_include.length)(0)
          var vect_index = 0
          for (genre_index <- genre_indexes_to_include) {
            if (p._2(pOfDay).contains(genre_index)) {
              deviceVector(vect_index) = p._2(pOfDay)(genre_index).toInt
            }
            vect_index += 1
          }

          "\"" + p._1 + "\"," + deviceVector.mkString(",")
        }
        )
        .map((1, _))

        if (FileSystem.get(new URI("s3n://magnet-fwm"), sc.hadoopConfiguration).exists(new Path(resultsPath))) {
          println("Deleting " + resultsPath)
          FileSystem.get(new URI("s3n://magnet-fwm"), sc.hadoopConfiguration).delete(new Path(resultsPath), true)
        }

        val matrixWithHeaders = header.union(devicesWV).sortByKey()
        matrixWithHeaders
          .map(_._2)
          .coalesce(1)
          .saveAsTextFile(resultsPath, classOf[GzipCodec])
      }
    }

    //</editor-fold>

    println("done!")

    //</editor-fold>
  }
}

// results:{
//  "Abilene-Sweetwater":6,
//  "Lake Charles":6,
//  "Seattle-Tacoma":8
// }

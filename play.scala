package SetTopBoxes

import java.io.File
import java.text.SimpleDateFormat
import java.time.DayOfWeek
import java.util.{Calendar, Locale}

import SetTopBoxes.Clustering.KMeansRun.Metrix
import SetTopBoxes.Clustering.KMeansRun.Metrix.euclidean
import breeze.linalg.functions.euclideanDistance
import org.apache.hadoop.io.compress.GzipCodec
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ListBuffer
import scala.util.control.Breaks.{break, breakable}

object play {
  val home_path = "data/"
  val home_path2 = "s3a://magnet-fwm/home/TsviKuflik/"
  val source_path = "/media/yuri/New Volume/SintecMediaData/"
  val source_path_windows = "D:\\SintecMediaData\\"

  @transient val conf: SparkConf = new SparkConf().setMaster("local").setAppName("Devices")
  @transient val sc: SparkContext = new SparkContext(conf)

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
      format.parse("25/05/2015"), // 25/5/2015 - Memorial day
      format.parse("03/07/2015"), format.parse("04/07/2015"), // 3/7/2015-4/7/2015 - Independence day
      format.parse("07/09/2015"), // 7/9/2015 - Labor day
      format.parse("11/09/2015"), // 11/9/2015 - September 11 attack
      format.parse("12/10/2015"), // 12/10/2015 - Columbus day
      format.parse("11/11/2015"), // 11/11/2015 - Veterans day
      format.parse("26/11/2015") // 26/11/2015 - Thanksgiving day
    )
    private val events: List[java.util.Date] = List[java.util.Date](
      format.parse("14/01/2015"), // 14/1/2015 - A collision between a train and a prison transport bus near Penwell, Texas.
      format.parse("26/01/2015"), format.parse("27/01/2015"), // 26-27/1/2015 - A blizzard hits the Northeast shutting down major cities including New York City and Boston, with up to 60 million people affected.
      format.parse("03/02/2015"), // 3/2/2015 - A collision between a commuter train and a passenger vehicle kills six in Valhalla, New York.
      format.parse("10/02/2015"), // 10/2/2015 - Chapel Hill shooting. Craig Stephen Hicks killed a Muslim family of three in Chapel Hill, North Carolina.
      format.parse("26/02/2015"), // 26/2/2015 - A gunman kills seven people then himself in a series of shootings in Tyrone, Missouri.
      format.parse("04/04/2015"), // 4/4/2015 -  Walter Scott, an unarmed man, is shot and killed by a police officer in North Charleston.
      format.parse("25/04/2015"), format.parse("26/04/2015"), format.parse("27/04/2015"), format.parse("28/04/2015"),
      format.parse("29/04/2015"), format.parse("30/04/2015"), format.parse("01/05/2015"),
      format.parse("02/05/2015"), format.parse("03/05/2015"), // 25/4/2015 - 3/5/2015 - Protests in Baltimore, Maryland from Baltimore City Hall to the Inner Harbor against the April 19 death of Freddie Gray in police custody soon turn violent, with extensive property damage
      format.parse("03/05/2015"), // 3/5/2015 - Two suspected Islamist gunmen attack the Curtis Culwell Center in the city of Garland, Texas.
      format.parse("12/05/2015"), // 12/5/2015 - An Amtrak train derails in the Philadelphia neighborhood for Port Richmond, causing cars to roll over and killing at least 8 people and injuring over 200.
      format.parse("17/05/2015"), // 17/5/2015 - A shootout erupts between rival biker gangs in a Twin Peaks restaurant in Waco, Texas, leaving nine dead.
      format.parse("20/05/2015"), // 20/5/2015 - David Letterman broadcasts the last episode of his 22-year run as host of The Late Show on CBS, drawing a record audience.
      format.parse("23/05/2015"), format.parse("24/05/2015"), format.parse("25/05/2015"), // 23-25/5/2015 - * Historic flash flooding levels occur in Texas and Oklahoma in a prolonged outbreak of floods and tornadoes, leaving at least 17 people dead and at least 40 others missing.
      format.parse("03/06/2015"), format.parse("06/06/2015"), format.parse("08/06/2015"), format.parse("10/06/2015"),
      format.parse("13/06/2015"), format.parse("15/06/2015"), // [3,6,8,10,13,15]/6/2015 - NHL, 2015 Stanley Cup Finals.
      format.parse("17/06/2015"), // 17/6/2015 - * Nine people are shot and killed during a prayer service at Emanuel African Methodist Episcopal Church, a historically black church
      format.parse("05/07/2015"), // 5/7/2015 -  2015 FIFA Women's World Cup
      format.parse("16/07/2015"), // 16/7/2015 - A gunman attacks two military installations in Chattanooga, Tennessee. Five U.S. Marines are killed and two others are injured.
      format.parse("17/07/2015"), format.parse("18/07/2015"), format.parse("19/07/2015"), format.parse("20/07/2015"),
      format.parse("21/07/2015"), // 17-21/7/2015 - The Cajon Pass wildfire spreads across 4,250 acres (1,720 ha) in the Mojave Desert near the towns of Victorville and Hesperia, north of San Bernardino and south of Bakersfield in the state of California
      format.parse("23/07/2015"), // 23/7/2015 - A gunman opens fire at a movie theater in Lafayette, Louisiana, killing two people and injuring nine others before committing suicide.
      format.parse("26/08/2015"), // 26/8/2015 - News reporter Alison Parker and camera operator Adam Ward are shot and killed on live television during an interview in Moneta, Virginia.
      format.parse("12/09/2015"), format.parse("13/09/2015"), // 12-13/9/2015 - The Valley wildfire claims at least three lives in Lake County, California with thousands of people forced to evacuate.
      format.parse("14/09/2015"), // 14/9/2015 - 2015 Utah floods, by Hurricane Linda.
      format.parse("20/09/2015"), // 20/9/2015 - The 67th Primetime Emmy Awards are held at the Microsoft Theater in Los Angeles, California.
      format.parse("01/10/2015"), // 1/10/2015 - 26-year-old Christopher Harper-Mercer opens fire at Umpqua Community College in Roseburg, Oregon. killing 9 people and injuring 9 others.
      format.parse("01/10/2015"), // 1/10/2015 - The SS El Faro, a cargo ship, sinks off the Bahamas after leaving Jacksonville, Florida two days prior, headed to Puerto Rico.
      format.parse("25/10/2015"), // 25/10/2015 - A drunk driver plows into the Oklahoma State Homecoming parade in Stillwater, Oklahoma, killing four people and injuring 34.
      format.parse("27/10/2015"), // 27/10/2015-1/11/2015 - World Series, 111th edition of Major League Baseball's championship series.
      format.parse("22/11/2015"), // 22/11/2015 - The 2015 New Orleans shooting took place at Bunny Friend playground in the Ninth Ward of New Orleans, Louisiana
      format.parse("27/11/2015"), // 27/11/2015 - A gunman opens fire at a Planned Parenthood clinic in Colorado Springs, Colorado, killing 3, including a police officer, and injuring 9.
      format.parse("02/12/2015"), // 2/12/2015 - 2015 San Bernardino attack: 14 people are killed in a terrorist attack at a facility for the mentally disabled in San Bernardino, California.
      format.parse("15/12/2015") // 15/12/2015 - 2015 Los Angeles Unified School District closure: The Los Angeles Unified School District received a credible terrorism threat causing the temporary closure of all Los Angeles Unified Schools.
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
      if(date==null){
        return false
      }
      IsInHolidays(date) || IsInEvents(date)
    }

    def IsInHolidays(date: java.sql.Timestamp): Boolean = {
      if(date==null){
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
      if(date==null){
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

  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "C:\\winutils");
    testToMap()
    //FlatRefXmlData()
  }

  def testToMap(): Unit = {

    val dates = SpecailDates.GetHolidays()

    val format = new SimpleDateFormat("yyyyMMddHHmmss")
    val t1 = new java.sql.Timestamp(format.parse("20150101083045").getTime)
    val t2 = new java.sql.Timestamp(format.parse("20150102083045").getTime)
    val t3 = new java.sql.Timestamp(format.parse("20150101000000").getTime)

    val t1b = SpecailDates.IsInDates(t1)
    val t2b = SpecailDates.IsInDates(t2)
    val t3b = SpecailDates.IsInDates(t3)

    val rrr = 0

  }

  def DataFramesTest(): Unit = {

    val conf = new SparkConf().setAppName("magnet").setMaster("local")
    var sc = new SparkContext(conf)
    sc.hadoopConfiguration.set("fs.s3a.access.key", "")
    sc.hadoopConfiguration.set("fs.s3a.secret.key", "")
    val file = sc.textFile(home_path + "rpt_programs/SintecMedia.rpt_programs.date_2015-01-01.2016-11-21.pd.gz")

    import org.apache.spark.sql.Row
    val rdd = file.map(_.split('|')).map(e ⇒ Row(e(0), e(1), e(2), e(3), e(4), e(5).trim.toInt))

    import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType};
    val schema = StructType(Array(
      StructField("prog_code", StringType, false),
      StructField("prog_title", StringType, false),
      StructField("prog_genres", StringType, false),
      StructField("air_date", StringType, false),
      StructField("air_time", StringType, false),
      StructField("duration", IntegerType, false)))

    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()

    val df = spark.createDataFrame(rdd, schema)

    df.show()
  }

  def CopyOneMonthViews(): Unit = {

    val conf = new SparkConf().setAppName("magnet").setMaster("local")
    var sc = new SparkContext(conf)
    sc.hadoopConfiguration.set("fs.s3a.access.key", "")
    sc.hadoopConfiguration.set("fs.s3a.secret.key", "")
    val file = sc.textFile(home_path + "rpt_prog_view/SintecMedia.rpt_prog_view.date_2015-01-*.2016-11-21.pd.gz")

    println("file size is " + file.count())

    //println(getListOfFiles(home_path + "rpt_programs/").filter(_.getName.contains("2015-01-01")))
  }

  def ProgramsGenres(): Unit = {
    val conf = new SparkConf().setAppName("magnet").setMaster("local")
    var sc = new SparkContext(conf)
    sc.hadoopConfiguration.set("fs.s3a.access.key", "")
    sc.hadoopConfiguration.set("fs.s3a.secret.key", "")
    val programs = sc.textFile(home_path + "rpt_programs/*.gz")

    val programs_rdd = programs.map(line => (line.split('|').lift(0).get + "|" + line.split('|').lift(2).get, 1))
      .reduceByKey(_ + _)
      .map(_._1)

    programs_rdd.foreach(println(_))
    //programs_rdd.saveAsTextFile(home_path + "output/test")
  }

  def OneMonthViewsWithGenres(): Unit = {

    val conf = new SparkConf().setAppName("magnet").setMaster("local")
    var sc = new SparkContext(conf)
    sc.hadoopConfiguration.set("fs.s3a.access.key", "")
    sc.hadoopConfiguration.set("fs.s3a.secret.key", "")
    val views = sc.textFile(home_path + "rpt_prog_view/SintecMedia.rpt_prog_view.date_2015-01-*.2016-11-21.pd.gz")
    val programs = sc.textFile(home_path + "playground/map_prog_to_genres/part*")

    val views_rdd = views.map(line => (line.split('|').lift(5).get, line))
    val programs_rdd = programs.map(line => (line.split('|').lift(0).get, line))

    val joined = views_rdd.leftOuterJoin(programs_rdd).map(t => t._2._1 + "|" + t._2._2.getOrElse("None"))

    //joined.take(20).foreach(println(_))

    joined.saveAsTextFile(home_path + "playground/one_month_views_with_genres", classOf[GzipCodec])

  }

  def getGenresFromPrograms(): Unit = {

    val conf = new SparkConf().setAppName("magnet").setMaster("local")
    var sc = new SparkContext(conf)
    val files = sc.wholeTextFiles(home_path + "rpt_programs/*.gz")

    val map = files.flatMap { case (filename, content) => scala.io.Source.fromString(content).getLines }
      .flatMap(line => line.split('|').lift(2).get.split(','))
      .map(word => (word, 1))
      .reduceByKey(_ + _)

    map.foreach(c => println(c))

    val sparkSession = SparkSession.builder.
      master("local")
      .appName("spark session example")
      .getOrCreate()

    val df = sparkSession.createDataFrame(map)
    df.write.format("com.databricks.spark.csv").save(home_path + "playground/genres_counts.csv")
  }

  def CountFiles(): Unit = {

    val conf = new SparkConf().setAppName("magnet").setMaster("local")
    var sc = new SparkContext(conf)
    val views = sc.textFile(home_path + "rpt_prog_view/SintecMedia.rpt_prog_view.date_2015-01-*.gz")
  }

  def FlatRefXmlData(): Unit = {

    val conf = new SparkConf().setAppName("magnet").setMaster("local")
    var sc = new SparkContext(conf)

    val xml = sc.textFile(source_path_windows + "refxml/SintecMedia.rpt_refxml.date_2015-01-01.2016-11-21.xml")

    def ListFromXML(content: String): String = {
      val _xml = scala.xml.XML.load(content)
      val list = new StringBuilder()
      (_xml \\ "mso-table").foreach { mso =>
        // <mso-table mso="01540" mso-name="Blade Communications">
        (mso \\ "mapping").foreach { h =>
          //<mapping device-id="00000008354e" household-id="01522829" household-type="FWM-ID" zipcode="43434" dma="Toledo" dma-code="547" system-type="H"/>
          list.append(mso.attribute("mso").getOrElse("") + "|" + mso.attribute("mso-name").getOrElse("") + "|" + h.attribute("device-id").getOrElse("") + "|" +
            h.attribute("household-id").getOrElse("") + "|" + h.attribute("household-type").getOrElse("") + "|" + h.attribute("zipcode").getOrElse("") + "|" +
            h.attribute("dma").getOrElse("") + "|" + h.attribute("dma-code").getOrElse("") + "|" + h.attribute("system-type").getOrElse(""))
        }
      }
      return list.toString()
    }

    //    val lines = xml.map{
    //      case (filename, content) =>
    //        //ListFromXML(content.toString())
    //    }
    //lines.saveAsTextFile(home_path + "playground/householders-csv/SintecMedia.rpt_refxml.date_2015-01-01.2016-11-21/")
  }

  def collections(): Unit = {

    val l = List(1, 2, 3, 4, 5)
    println(l)
    println(l(1))

    println(l.map(x => x * 2))

    def g(v: Int) = List(v - 1, v, v + 1)

    println(l.flatMap(x => g(x)))

    val m = Map(1 -> 2, 2 -> 4, 3 -> 6)
    println(m)
    println(m.toList)
    println(m.contains(55) + " or " + m.keySet.contains(55))

    val t = (1, 2)
    println(t)

    // map only values
    print(m.mapValues(v => v * 2))

  }


  // Helpers
  def getListOfFiles(dir: String): List[File] = {
    val d = new File(dir)
    if (d.exists && d.isDirectory) {
      d.listFiles.filter(_.isFile).toList
    } else {
      List[File]()
    }
  }
}
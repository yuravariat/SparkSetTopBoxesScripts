package SetTopBoxes

import java.text.SimpleDateFormat
import java.util.concurrent.TimeUnit

import scala.collection.mutable.ListBuffer


/**
  * Created by yurav on 05/02/2017.
  */
object main {
  def main(args: Array[String]): Unit = {
    val ttt = 1
    print("hello" + ttt)

    val intervalsMinutes = 10
    val format = new SimpleDateFormat("yyyyMMddHHmmss")
    val formatHours = new SimpleDateFormat("HH")
    val limitDateForLearn = 20150601
    val startTime = new java.sql.Timestamp(format.parse(limitDateForLearn.toString + "000000").getTime)
    val timeSlots: ListBuffer[(String, Int)] = ListBuffer()

    val intervals = (60 / intervalsMinutes) + (if (60 % intervalsMinutes != 0) 1 else 0)
    println("intervals " + intervals)
    println("startTime " + startTime)

    for (i <- 0 to intervals) {
      val minutes = (startTime.getMinutes/intervalsMinutes) * intervalsMinutes
      println("minutes " + minutes)
      timeSlots.append((formatHours.format(startTime) + ":" + (if (minutes==0) "00" else minutes.toString),1))
      startTime.setTime(startTime.getTime + TimeUnit.MINUTES.toMillis(intervalsMinutes))
      println("startTime " + startTime)
    }

    println("done!")

  }
  def getGenresCountsFromPrograms():Unit = {

    import org.apache.spark.{SparkConf, SparkContext}
    import org.apache.spark.sql.SparkSession

    //sc.stop() //open for spark shell
    val conf = new SparkConf().setAppName("magnet").setMaster("local")
    var sc = new SparkContext(conf)
    sc.hadoopConfiguration.set("fs.s3a.access.key", "")
    sc.hadoopConfiguration.set("fs.s3a.secret.key", "")
    val files = sc.wholeTextFiles("s3a://magnet-fwm/home/TsviKuflik/rpt_programs/*.gz")

    val map = files.flatMap{case (filename,content) => scala.io.Source.fromString(content).getLines}
      .flatMap(line => line.split('|').lift(2).get.split(','))
      .map(word => (word, 1))
      .reduceByKey(_ + _)

    val sparkSession = SparkSession.builder.
      master("local")
      .appName("spark session example")
      .getOrCreate()

    val df = sparkSession.createDataFrame(map)
    df.write.format("com.databricks.spark.csv").save("s3://magnet-fwm/home/TsviKuflik/playground/genres_counts")

    //map.saveAsTextFile("s3://magnet-fwm/home/TsviKuflik/playground/genres_counts")

  }
}

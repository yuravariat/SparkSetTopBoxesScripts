package SetTopBoxes.Programs

import java.text.SimpleDateFormat

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by yurav on 19/04/2017.
  */
object TranslatedPrograms {

  val spark:SparkSession = SparkSession
    .builder()
    .master("local")
    .appName("Scala Spark SQL")
    .config("spark.sql.warehouse.dir", "file:///tmp/spark-warehouse")
    .getOrCreate();

  /** Main function */
  def main(args: Array[String]): Unit = {
    @transient val conf: SparkConf = new SparkConf().setMaster("local").setAppName("Devices")
    @transient val sc: SparkContext = new SparkContext(conf)

    case class Program(
      prog_id: String,
      name:String,
      genres:String,
      start_date:String,
      start_time:String,
      start_date_time:java.sql.Timestamp,
      prog_duration:Double
    ) extends Serializable

    val format = new SimpleDateFormat("yyyyMMddHHmmss")
    val simpleFormat = new SimpleDateFormat("dd-MM-yyyy")
    val dateLimitFrom = new java.sql.Timestamp(format.parse("20150331000000").getTime)
    val dateLimitTill = new java.sql.Timestamp(format.parse("20150502000000").getTime)

    for(i <- 0 until 1) { // Dummy for

      val programs = sc.textFile("s3n://magnet-fwm/home/TsviKuflik/rpt_programs/SintecMedia.rpt_programs.date_2015-03-31*.gz," +
        "s3n://magnet-fwm/home/TsviKuflik/rpt_programs/SintecMedia.rpt_programs.date_2015-04*.gz," +
        "s3n://magnet-fwm/home/TsviKuflik/rpt_programs/SintecMedia.rpt_programs.date_2015-05-01*.gz")
        .map(s => {
          val sp = s.split("\\|")
          val dt = new java.sql.Timestamp(format.parse(sp(3)+sp(4)).getTime)
          Program(sp(0),sp(1),sp(2),sp(3),sp(4),dt,sp(5).toDouble)
        })
        .sortBy(d => d.start_date + d.start_time)

      programs
        .map(p=> p.prog_id + "|" + p.name + "|" + p.genres + "|" + p.start_date+ "|" + p.start_time
          + "|" + p.start_date_time+ "|" + p.prog_duration)
        .coalesce(1)
        .saveAsTextFile("s3n://magnet-fwm/home/TsviKuflik/y_programs/translated_"+
          simpleFormat.format(dateLimitFrom) + "_" + simpleFormat.format(dateLimitTill))
    }

    println("done!")

  }
}

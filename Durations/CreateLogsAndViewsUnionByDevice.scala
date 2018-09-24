package SetTopBoxes.Durations

import java.text.SimpleDateFormat

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by yurav on 19/04/2017.
  * spark-shell --driver-memory 10G --executor-memory 18G -i test.scala
  * nohup spark-shell --driver-memory 10G --executor-memory 15G -i test.scala >> out.log &
  */
object CreateLogsAndViewsUnionByDevice {

  case class ViewingData(
                          device_id: String,
                          log_type: Int, // 0 - view, 1- log
                          event_date_time: java.sql.Timestamp,
                          station_num: String,
                          prog_code: String,
                          var duration: Int
                        ) extends Serializable

  /** Main function */
  def main(args: Array[String]): Unit = {
    @transient val conf: SparkConf = new SparkConf().setMaster("local").setAppName("Devices")
    @transient val sc: SparkContext = new SparkContext(conf)

    val spark: SparkSession = SparkSession
      .builder()
      .master("local")
      .appName("Scala Spark SQL")
      .config("spark.sql.warehouse.dir", "file:///tmp/spark-warehouse")
      .getOrCreate()

    import spark.implicits._

    val format = new SimpleDateFormat("yyyyMMddHHmmss")

    val programs_path = "s3n://magnet-fwm/home/TsviKuflik/rpt_programs/SintecMedia.rpt_programs*.gz"
    val views_path = "s3n://magnet-fwm/home/TsviKuflik/rpt_prog_view/SintecMedia.rpt_prog_view*.gz"
    val logs_path = "s3n://magnet-fwm/home/TsviKuflik/rawxml/*.gz"

    val results_path = "s3n://magnet-fwm/home/TsviKuflik/y_views_logs_union_by_device"

    val programsDurations = sc.textFile(programs_path)
      //.filter(s=>{!s.isEmpty && s.split("\\|").length>5})
      .map(s => {
      //0-prog_code
      //1-prog_title
      //2-prog_genres
      //3-air_data
      //4-air_time
      //5-duration
      val sp = s.split("\\|")
      val prog_id = sp(0)
      (prog_id, sp(5).toDouble.toInt)
    })
      .reduceByKey((a, b) => {
        if (a > b) {
          a
        } else {
          b
        }
      }).collect().toMap

    println("programs sample:")
    //programsDurations.take(3).foreach(p => println(p))

    val views = sc.textFile(views_path)
      //.filter(s=>{!s.isEmpty && s.split("\\|").length>5})
      .map(s => {
      //0-msoCode
      //1-deviceID
      //2-date
      //3-time
      //4-station
      //5-prog_code
      // sample: 01540|0000000057f6|20150104|054745|14909|SH000000010000
      val sp = s.split("\\|")
      val dt = new java.sql.Timestamp(format.parse(sp(2) + sp(3)).getTime)
      val prog = programsDurations.get(sp(5))
      ViewingData(sp(0) + "-" + sp(1), 0, dt, sp(4), sp(5), if (prog.isDefined) prog.get else 0)
      //ViewingData(sp(0) + "-" + sp(1), 0, dt, sp(4), sp(5), "", "", "", 0, 0, if (prog.isDefined) prog.get else 0)
    })

    println("views sample:")
    //views.take(3).foreach(p=>println(p))

    val logs = sc.textFile(logs_path) // Collect only OFF events
      .filter(s => {
      !s.isEmpty && s.split("\\|")(6) == "OFF"
    })
      .map(s => {
        //0-msoCode
        //1-deviceID
        //2-date
        //3-time
        //4-event_type
        //5-event_value
        //6-event_name
        //7-event_id
        //sample: 01540|0000000057f6|20150105|102338|O|0|OFF|
        //        01540|0000000057f6|20150105|191234|T|199|GSN|14909
        val sp = s.split("\\|")
        val dt = new java.sql.Timestamp(format.parse(sp(2) + sp(3)).getTime)
        ViewingData(sp(0) + "-" + sp(1), 1, dt, "", sp(6), 0)
        //ViewingData(sp(0) + "-" + sp(1), 1, dt, "", "", sp(4), sp(5), sp(6), if (sp.length > 7) sp(7).toInt else 0, 0, 0)
        //ViewingData(sp(0) + "-" + sp(1), LOG, dt, null, null, null, null, sp(6), 0, 0, 0)
      })

    println("logs sample:")
    //logs.take(3).foreach(p=>println(p))

    println("saving results ...")
    val union = views.union(logs).toDF()

    union.write
      .partitionBy("device_id")
      .format("com.databricks.spark.csv")
      .option("codec", "org.apache.hadoop.io.compress.GzipCodec")
      .save(results_path)

    //.saveAsTextFile(results_path, classOf[GzipCodec])

    print("done!")
  }
}

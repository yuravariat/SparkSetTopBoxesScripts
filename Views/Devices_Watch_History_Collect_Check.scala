package SetTopBoxes.Views

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by yurav on 19/04/2017.
  */
object Devices_Watch_History_Collect_Check {

  @transient val conf: SparkConf = new SparkConf().setMaster("local").setAppName("Devices")
  @transient val sc: SparkContext = new SparkContext(conf)
  case class ViewingData(
       device_id_unique:String,
       log_type:Int,
       event_date:String,
       event_time:String,
       station_num:String,
       prog_code:String,
       event_type:String,
       event_value:String,
       event_name:String,
       event_id:Int
   ) extends Serializable

  /** Main function */
  def main(args: Array[String]): Unit = {

    val logsAndViewsRDD = sc.textFile("s3n://magnet-fwm/home/TsviKuflik/devices_views/devices_120_hist/*")
      .map(line=>{
        val sp = line.split("\\|")
        new ViewingData(sp(0),sp(1).toInt,sp(2),sp(3),sp(4),sp(5),sp(6),sp(7),sp(8),sp(9).toInt)
      })

    logsAndViewsRDD.count()

    logsAndViewsRDD
         .map(v=>(v.event_date,1))
         .reduceByKey(_+_)
         .collect()
         .foreach(v=>println(v))

    logsAndViewsRDD
         .map(v=>(v.device_id_unique,1))
         .reduceByKey(_+_)
         .count()

    logsAndViewsRDD
         .map(v=>(v.log_type,1))
         .reduceByKey(_+_)
         .collect()
         .foreach(v=>println(v))

    print("done!")

  }
}
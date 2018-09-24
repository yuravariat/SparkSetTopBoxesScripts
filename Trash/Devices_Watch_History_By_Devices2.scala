package SetTopBoxes.Trash

import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

/**
  * Created by yurav on 19/04/2017.
  */
object Devices_Watch_History_By_Devices2 {

  @transient val conf: SparkConf = new SparkConf().setMaster("local").setAppName("Devices")
  @transient val sc: SparkContext = new SparkContext(conf)

  /** Main function */
  def main(args: Array[String]): Unit = {

    class RddMultiTextOutputFormat extends MultipleTextOutputFormat[Any, Any] {
      override def generateActualKey(key: Any, value: Any): Any = null
      override def generateFileNameForKeyValue(key: Any, value: Any, name: String): String = key.asInstanceOf[String]
    }
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

    val logsAndViewsRDD = sc.textFile("s3n://magnet-fwm/home/TsviKuflik/devices_views/devices_120_hist/*")
      .map(line=>{
        val sp = line.split("\\|")
        (sp(0),sp(1)+"|"+sp(2)+"|"+sp(3)+"|"+sp(4)+"|"+sp(5)+"|"+sp(6)+"|"+sp(7)+"|"+sp(8)+"|"+sp(9))
        //(sp(0),new ViewingData(sp(0),sp(1).toInt,sp(2),sp(3),sp(4),sp(5),sp(6),sp(7),sp(8),sp(9).toInt))
      })
      .groupByKey()
      //.mapValues(v=>v.toList.sortBy(l=>l.event_date+l.event_time))
      .mapValues(v=>v.toList.sortBy { case line =>
          val sp = line.split("\\|")
          sp(1) + sp(2)
        }
      )
      .partitionBy(new HashPartitioner(15))
      .saveAsTextFile("s3n://magnet-fwm/home/TsviKuflik/devices_views/devices_120_hist_by_device")

    print("done!")

  }
}
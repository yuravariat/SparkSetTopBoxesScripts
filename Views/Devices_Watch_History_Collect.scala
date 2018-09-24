package SetTopBoxes.Views

import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

/**
  * Created by yurav on 19/04/2017.
  */
object Devices_Watch_History_Collect {

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

    val cities = List[String]("Abilene-Sweetwater","Lake Charles","Seattle-Tacoma")
    val files = new Array[String](3)
    var devices = List[(String,String)]() // List of all devices from all cities.

    for(i <- files.indices){
      val city_name = cities(i)
      files(i)="s3n://magnet-fwm/home/TsviKuflik/devices/devices_120_hist_" + city_name + "/*.gz"

      devices = devices ::: sc.textFile(files(i))
        .filter(l=>l.split(",")(0)!="deviceIUniqueD")
        .map(line=>{
          val sp = line.split(",")
          (city_name,sp(0))
        }).collect().toList
    }

    val device_ids = devices.map(_._2)

    val viewsRDD = sc.textFile("s3n://magnet-fwm/home/TsviKuflik/rpt_prog_view/SintecMedia.rpt_prog_view.date_2015-01*.gz," +
      "s3n://magnet-fwm/home/TsviKuflik/rpt_prog_view/SintecMedia.rpt_prog_view.date_2015-02*.gz," +
      "s3n://magnet-fwm/home/TsviKuflik/rpt_prog_view/SintecMedia.rpt_prog_view.date_2015-03*.gz," +
      "s3n://magnet-fwm/home/TsviKuflik/rpt_prog_view/SintecMedia.rpt_prog_view.date_2015-04*.gz," +
      "s3n://magnet-fwm/home/TsviKuflik/rpt_prog_view/SintecMedia.rpt_prog_view.date_2015-05-0*.gz")
      .filter { case (x) =>
        if (x.isEmpty) {
          false
        }
        else {
          val sp = x.split("\\|")
          device_ids.contains(sp(0)+"-"+sp(1)) && sp(3).toInt<20150402
        }
      }
      .map(line=>{
        val sp = line.split("\\|")
        ViewingData(sp(0)+"-"+sp(1),0,sp(2),sp(3),sp(4),sp(5),"","","",0)
      })

//    viewsRDD.map(l=>
//      l.device_id_unique+"|"+l.log_type+"|"+l.event_date +
//        "|" +  l.event_time+"|"+l.station_num+"|"+l.prog_code+
//        "|" + l.event_type+"|"+l.event_value+"|"+l.event_name + "|" + l.event_id
//    ).take(10).foreach(println(_))

    val logsRDD = sc.textFile("s3n://magnet-fwm/home/TsviKuflik/rawxml/FWM_201501*.gz," +
      "s3n://magnet-fwm/home/TsviKuflik/rawxml/FWM_201502*.gz," +
      "s3n://magnet-fwm/home/TsviKuflik/rawxml/FWM_201503*.gz," +
      "s3n://magnet-fwm/home/TsviKuflik/rawxml/FWM_201504*.gz," +
      "s3n://magnet-fwm/home/TsviKuflik/rawxml/FWM_2015050*.gz")
      .filter { case (x) =>
        if (x.isEmpty) {
          false
        }
        else {
          val sp = x.split("\\|")
          device_ids.contains(sp(0)+"-"+sp(1)) && sp(3).toInt<20150402
        }
      }
      .map(line=>{
        val sp = line.split("\\|")
        ViewingData(sp(0)+"-"+sp(1),1,sp(2),sp(3),"","",sp(4),sp(5),sp(6)
          , if (sp.length > 7) sp(7).toInt else 0)
      })

    val logsAndViewsRDD = viewsRDD.union(logsRDD).cache()

    for(i <- files.indices){
      val city_name = cities(i)
      val cityDevices = devices.filter(t=>t._1==city_name).map(_._2)

      logsAndViewsRDD
      .filter(l=>cityDevices.contains(l.device_id_unique))
      .map(l=>
          (l.device_id_unique,
            l.log_type+"|"+l.event_date + "|" +  l.event_time+"|"+l.station_num+"|"+l.prog_code+
            "|" + l.event_type+"|"+l.event_value+"|"+l.event_name + "|" + l.event_id)
      )
      .groupByKey()
      .mapValues( v => v.toList.sortBy { line =>
        val sp = line.split("\\|")
        sp(1) + sp(2)
      }
      )
      .partitionBy(new HashPartitioner(15))
      .saveAsTextFile("s3n://magnet-fwm/home/TsviKuflik/devices_views/devices_120_hist_" + city_name + "_ordered")
    }


    print("done!")

  }
}
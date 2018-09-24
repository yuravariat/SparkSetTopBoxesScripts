package SetTopBoxes.Durations

import java.sql.Timestamp
import java.text.SimpleDateFormat

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by yurav on 19/04/2017.
  */
object Calculate_durations_test {
  @transient val conf: SparkConf = new SparkConf().setMaster("local").setAppName("Devices")
  @transient val sc: SparkContext = new SparkContext(conf)
  val spark:SparkSession = SparkSession
    .builder()
    .master("local")
    .appName("Scala Spark SQL")
    .config("spark.sql.warehouse.dir", "file:///tmp/spark-warehouse")
    .getOrCreate();

  case class ViewingData(
    device_id_unique:String,
    log_type:Int,
    event_date_time:Timestamp,
    station_num:String,
    prog_code:String,
    event_type:String,
    event_value:String,
    event_name:String,
    event_id:Int
  ) extends Serializable

  /** Main function */
  def main(args: Array[String]): Unit = {

    val cities = List[(String,Int)](("Abilene-Sweetwater",6))

    for(i <- 0 until cities.length) {
      val cityname = cities(i)._1
      val devicesSource = "D:\\SintecMediaData\\devices_views\\devices_120_hist_Seattle-Tacoma_ordered\\part-00000"

      val devicesWithViews = sc.textFile(devicesSource)
        .map(x=> {
          val firstComma = x.indexOf(",")
          val device_id = x.substring(1,firstComma)
          val viewsStrings = x.substring(firstComma+6,x.length-2).split(",")
          val format = new SimpleDateFormat("yyyyMMddHHmmss")
          val views = viewsStrings.map(s=>{
            val sp = s.trim.split("\\|")
            val dt = new Timestamp(format.parse(sp(1)+sp(2)).getTime)
            new ViewingData("d",sp(0).toInt,dt,sp(3),sp(4),sp(5),sp(6),sp(7),sp(8).toInt)
          }).toList
          (device_id,views)
        }
        )

      devicesWithViews.take(1).foreach(r=>{
        println(r._1)
        r._2.foreach(v=>{
          println(v)
        })
      })
    }

//    groupedByDevicesDF.write
//      .coalesce(25)
//      .format("com.databricks.spark.csv")
//      .option("codec", "org.apache.hadoop.io.compress.GzipCodec")
//      .save("D:\\SintecMediaData\\devices\\devices_appearances")

  }
}

// results:{
//  "Abilene-Sweetwater":6,
//  "Lake Charles":6,
//  "Seattle-Tacoma":8
// }

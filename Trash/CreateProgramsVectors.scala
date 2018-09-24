package SetTopBoxes.Trash

import java.text.SimpleDateFormat

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

/**
  * Created by yurav on 19/04/2017.
  */
object CreateProgramsVectors {

  val spark:SparkSession = SparkSession
    .builder()
    .master("local")
    .appName("Scala Spark SQL")
    .config("spark.sql.warehouse.dir", "file:///tmp/spark-warehouse")
    .getOrCreate()

  case class ViewingData(
    event_date_time:java.sql.Timestamp,
    time:Int,
    station_num:String,
    prog_code:String,
    duration:Int,
    prog_duration:Int
  ) extends Serializable

  /** Main function */
  def main(args: Array[String]): Unit = {
    @transient val conf: SparkConf = new SparkConf().setMaster("local").setAppName("Devices")
    @transient val sc: SparkContext = new SparkContext(conf)

    val cities = List[(String,Int)](("Abilene-Sweetwater",6),("Lake Charles",6),("Seattle-Tacoma",8))
    val format = new SimpleDateFormat("yyyyMMddHHmmss")
    val partsOfDay = 1
    val notAllowedPRograms = List("MP_MISSINGPROGRAM","DVRPGMKEY"
    ,"EP000009937449","SH000191120000","SH000000010000","H002786110000","SH015815970000","SH000191160000"
    ,"SH000191300000","SH000191680000","SH001083440000","SH003469020000","SH007919190000","SH008073210000"
    ,"SH014947440000","SH016938330000","SH018049520000","SH020632040000","SP003015230000")

    val programs = sc.textFile("s3n://magnet-fwm/home/TsviKuflik/y_programs/programs_groups/part*")
      .map(s=>{
        val sp = s.split("\\|")
        val prog_name = sp(0)
        val name_ids = ListBuffer[String]()
        for(i<-1 until sp.length){
          name_ids.append(sp(i))
        }
        (prog_name,name_ids)
      })
      .collect()
      .sortBy(s=>s._1)
      .toList

    var progsToIndex = programs
      .zipWithIndex
      .flatMap{case (p,index)=>{
      p._2.map(pr=>(pr,index))
    }}.toMap

//    progsToIndex.take(50).foreach(m=>
//      println(m)
//    )
//  val logs = sc.textFile(base_path + "monitor/*.csv")
//    .filter(s=>{
//      val sp = s.split("\t")
//      //sp(5)=="35.195.110.141"
//      //sp(5)=="104.196.20.87" || sp(5)=="35.187.97.40"
//      sp(5)=="104.196.20.87" || sp(5)=="35.187.97.40" || sp(5)=="35.195.110.141"
//    })
//    .map(s=>{
//      val sp = s.split("\t")
//      val time = sp(0).substring(11,19)
//      var source = ""
//      if(sp(5)=="104.196.20.87" || sp(5)=="35.187.97.40"){
//        source = "site"
//      }
//      else{
//        source = "meta"
//      }
//      (time,(source,1))
//    })
//    .groupByKey()
//    .map(g=>{
//      val site_count = g._2.filter(_._1=="site").map(_._2).sum
//      val meta_count = g._2.filter(_._1=="meta").map(_._2).sum
//
//      (g._1, meta_count, site_count, site_count + meta_count)
//    })
//    .toDF("time","meta","site","total")

    for(i <- 0 until cities.length) {
      val city_name = cities(i)._1
      println("Working on " + city_name)
      val devicesSource = "s3n://magnet-fwm/home/TsviKuflik/devices_views/devices_120_hist_"+city_name+"_ord_dur/part*"
      val dateLimit = new java.sql.Timestamp(format.parse("20150401000000").getTime)

      val devicesWithViews = sc.textFile(devicesSource)
        .filter(s=>{
          if(s.isEmpty()){
            false
          }
          else{
            val firstComma = s.indexOf(",")
            val viewsStrings = s.substring(firstComma+6,s.length-2).split(",")
            viewsStrings.length>1500
          }
        })
        .map(x=> {
          val firstComma = x.indexOf(",")
          val device_id = x.substring(1,firstComma)
          val viewsStrings = x.substring(firstComma+6,x.length-2).split(",")
          val views = viewsStrings.map(s=>{
            val sp = s.trim.split("\\|")
            val dt = new java.sql.Timestamp(format.parse(sp(0).trim).getTime)
            val time_part:Int = sp(0).substring(8,12).toInt
            new ViewingData(dt,time_part,sp(1),sp(2),sp(3).toInt,sp(3).toInt)
          }).filter(p=> p.event_date_time.before(dateLimit) && p.duration>5
            && !notAllowedPRograms.contains(p.prog_code)).toList

          var vector:Array[Int] = Array.fill[Int](programs.length*partsOfDay)(0)
          var norm_vector:Array[Int] = Array.fill[Int](vector.length)(0)


          views.foreach(v=>{
            val pr = progsToIndex.get(v.prog_code)
            if(pr!=None) {
              var partOfDay = 1
              if(partsOfDay==4){
                if(v.time>=600 && v.time<1200){
                  partOfDay = 1
                }
                else if(v.time>=1200 && v.time<1900){
                  partOfDay = 2
                }
                else if(v.time>=1900 && v.time<2300){
                  partOfDay = 3
                }
                else{
                  partOfDay = 4
                }
              }
              var shift = programs.length*(partOfDay-1)
              vector(shift + pr.get)+=1
            }
          })

          // normalize f=> normalized = (value - min) / (max - min);
          val max = vector.max
          for(i <- 0 to (vector.length-1)){
            norm_vector(i) = ((vector(i)/max.toDouble)*100).toInt
          }

          (device_id,norm_vector)
        }
      )

      //devicesWithViews.map(p=> p._1 + "," + p._2.mkString(","))
      //  .coalesce(1)
      //  .saveAsTextFile("s3n://magnet-fwm/home/TsviKuflik/y_for_clustering/devices_120_hist_" + city_name + "_progs_vect_" + partsOfDay)

      // Add headers to create csv format.
      val headers:ListBuffer[String] = ListBuffer[String]("\"device_id\"")
      for(i<-0 to programs.length){
        headers.append("\"prog_" + i + "\"")
      }
      val headerStr :String = headers.mkString(",")
      val header = sc.parallelize(List((0,headerStr)))
      val devicesWV = devicesWithViews.map(p=> "\"" + p._1.split("-")(1) + "\"," + p._2.mkString(",")).map((1,_))
      val matrixWithHeaders = header.union(devicesWV).sortByKey()
      matrixWithHeaders
        .map(_._2)
        .coalesce(1)
        .saveAsTextFile("s3n://magnet-fwm/home/TsviKuflik/y_for_clustering/devices_120_hist_" + city_name + "_progs_vect_" + partsOfDay)

    }
    println("done!")
  }
}

// results:{
//  "Abilene-Sweetwater":6,
//  "Lake Charles":6,
//  "Seattle-Tacoma":8
// }

// Remove suspicies programs :
// EP000009937449|College Football|Sports event,Football,Playoff sports|20150102|060000|120.0
// SH000191120000|SIGN OFF|Special|20150101|060000|480.0
// SH000000010000|Paid Programming|Shopping|20150331|000000|300.0
// H002786110000|CountyNet||20150102|140000|300.0
// SH015815970000|ViaCast TV|Variety|20150101|050000|300.0
// SH000191160000|ABC World News Now|News|20150101|071300|107.0
// SH000191300000|Up to the Minute|News|20150101|070000|120.0
// SH000191680000|To Be Announced|Special|20150101|043500|122.0
// SH001083440000|Classic Arts Showcase|Art|20150102|080000|300.0
// SH003469020000|PA Public Affairs|Public affairs|20150102|050000|420.0
// SH007919190000|Blazers Television Off-Air||20141231|200000|360.0
// SH008073210000|Movie|Special,Entertainment|20141231|230000|120.0
// SH014947440000|Weather Nation|Weather|20150102|140000|360.0
// SH016938330000|Película|Special,Entertainment|20150101|234500|110.0
// SH018049520000|24 Hour Local Weather & Information|Weather,Community|20150102|010000|240.0
// SH020632040000|¡Feliz 2015!|Special,Entertainment|20150101|010000|420.0
// SP003015230000|NHL Hockey|Sports event,Hockey|20150102|150000|180.0

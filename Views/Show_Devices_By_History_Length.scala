package SetTopBoxes.Views

/**
  * Created by yurav on 19/04/2017.
  */
object Show_Devices_By_History_Length {
  import org.apache.spark._

  case class Device(mso:String,
                    msoName:String,
                    deviceId:String,
                    householdId:String,
                    householdType:String,
                    zipcode:String,
                    city:String,
                    cityCode:String,
                    systemType:String,
                    periodStart:Int,
                    periodEnd:Int,
                    periodLength:Int,
                    daysCount:Int,
                    daysArray:String) extends Serializable
  @transient val conf: SparkConf = new SparkConf().setMaster("local").setAppName("Devices")
  @transient val sc: SparkContext = new SparkContext(conf)

  /** Main function */
  def main(args: Array[String]): Unit = {

    val lines = sc.textFile("D:\\SintecMediaData\\refxml\\AllDevicesTimeLine.csv\\")
    val CSVParserRegex = """,(?=(?:[^\"]*\"[^\"]*\")*(?![^\"]*\"))""".r

    val devices = lines.map(line => {
      val arr = CSVParserRegex.split(line)
      Device(
        mso=arr(0).replace("\"","").trim,
        msoName=arr(1).replace("\"","").trim,
        deviceId=arr(2).replace("\"","").trim,
        householdId= arr(3).replace("\"","").trim,
        householdType=arr(4).replace("\"","").trim,
        zipcode=arr(5).replace("\"","").trim,
        city=arr(6).replace("\"","").trim,
        cityCode="",//arr(7).replace("\"","").trim,
        systemType="",//arr(8).replace("\"","").trim,
        periodStart=0,//arr(9).toInt,
        periodEnd=0,//arr(10).toInt,
        periodLength=0,//arr(11).toInt,
        daysCount=0,//arr(12).toInt,
        daysArray=""//arr(13).replace("\"","").trim
      )
    })

    val counts = devices
      .map(d=>(d.city,1))
      .reduceByKey(_+_)
      .collect()
      .sortBy(-_._2)

    counts.foreach(c=>
      println(c._1 + " -> " + c._2)
    )
  }
}

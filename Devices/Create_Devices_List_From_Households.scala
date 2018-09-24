package SetTopBoxes.Devices

import java.net.URI

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by yurav on 19/04/2017.
  */
object Create_Unique_Devices_List {
  @transient val conf: SparkConf = new SparkConf().setMaster("local").setAppName("Devices")
  @transient val sc: SparkContext = new SparkContext(conf)

  val spark: SparkSession = SparkSession
    .builder()
    .master("local")
    .appName("Scala Spark SQL")
    .config("spark.sql.warehouse.dir", "file:///tmp/spark-warehouse")
    .getOrCreate()

  import spark.implicits._

  /** Main function */
  def main(args: Array[String]): Unit = {

    //<editor-fold desc="Run area">

    val resultsPath = "s3n://magnet-fwm/home/TsviKuflik/y_for_roni/devices"
    val houseHolds = sc.textFile("s3n://magnet-fwm/home/TsviKuflik/y_for_roni/households/ids.txt")
      .map(s=>s).collect()

    val devices = sc.textFile("s3n://magnet-fwm/home/TsviKuflik/y_devices/unique_devices/*.gz")
      .filter(s=>{
        val sp = s.split(",")
        val householdid = sp(4).replace("\"","").toInt
        houseHolds.contains(householdid.toString)
      })
      .map(s=>{
        val sp = s.split(",")
        sp(0) + "," + sp(4)
      })

    if(FileSystem.get(new URI("s3n://magnet-fwm"), sc.hadoopConfiguration).exists(new Path(resultsPath)))
    {
      println("Deleting " + resultsPath)
      FileSystem.get(new URI("s3n://magnet-fwm"), sc.hadoopConfiguration).delete(new Path(resultsPath), true)
    }

    devices.saveAsTextFile(resultsPath)

    println("done!")

    //</editor-fold>

  }
}

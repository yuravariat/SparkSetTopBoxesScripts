package SetTopBoxes.Matrix

import java.io.Serializable
import java.net.URI
import java.text.SimpleDateFormat
import java.util.concurrent.TimeUnit

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.compress.GzipCodec
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.{SparkConf, SparkContext}

//<editor-fold desc="Case classes">

class ViewingData(val device_id_unique: String,
                  val event_date_time: java.sql.Timestamp,
                  val prog_code: String,
                  val duration: Int) extends Serializable {

  override def toString: String = {
    val dt = new java.text.SimpleDateFormat("yyyyMMddHHmmss").format(event_date_time)
    s"$dt|$prog_code|$duration"
  }
}

//</editor-fold>

/**
  * Created by yurav on 19/04/2017.
  */
object CreateLogsForMatrix {

  val spark: SparkSession = SparkSession
    .builder()
    .master("local")
    .appName("Scala Spark SQL")
    .config("spark.sql.warehouse.dir", "file:///tmp/spark-warehouse")
    .getOrCreate()

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

    //<editor-fold desc="Run area">

    println("\n" + "#" * 70 + " CreateLogsForMatrix " + "#" * 70 + "\n")

    val cities = List("Amarillo", "Parkersburg", "Little Rock-Pine Bluff", "Seattle-Tacoma")
    val format = new SimpleDateFormat("yyyyMMddHHmmss")
    val formatMonthDay = new SimpleDateFormat("MMdd")
    val base_path = "s3n://magnet-fwm/home/TsviKuflik/"
    val logs_path = List(base_path + "rawxml/FWM_201501*_R.pd.gz",
      base_path + "rawxml/FWM_201502*_R.pd.gz",
      base_path + "rawxml/FWM_201503*_R.pd.gz",
      base_path + "rawxml/FWM_201504*_R.pd.gz",
      base_path + "rawxml/FWM_201505*_R.pd.gz")

    val predictionStart = new java.sql.Timestamp(format.parse("20150101000000").getTime)
    val predictionEnd = new java.sql.Timestamp(format.parse("20150601000000").getTime)

    def StringToDate = udf((date:String ) => {
      new java.sql.Timestamp(format.parse(date).getTime)
    })

    for (i <- cities.indices) {

      val city_name = cities(i).replace(" ","-")
      val devicesSource = base_path + "y_devices/5_months/" + city_name + "/part*"
      val results_path = base_path + "y_for_matrix/logs/5_months/" + city_name + "-" +
        formatMonthDay.format(predictionStart) + "-" + formatMonthDay.format(predictionEnd)

      val devicesForThisCity = sc.textFile(devicesSource).map(d => d.trim).collect()
      println("working on " + city_name + " devices in city " + devicesForThisCity.length)
      println("source="+devicesSource)
      println("dest="+results_path)
      // devices rdd (device_id,List[ViewingData]) Devices with their views

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

      val results = spark.read
        .format("com.databricks.spark.csv")
        .option("delimiter", "|")
        .load(logs_path:_*)
        .select(
            $"_c0".as("mso_code"),
            $"_c1".as("device_id"),
            $"_c2".as("date"),
            $"_c3".as("time"),
            $"_c4".as("type"),
            $"_c5".as("value"),
            $"_c6".as("name"),
            $"_c7".as("event_id"))
        .withColumn("deviceIdUniqe",concat_ws("-",$"mso_code", $"device_id"))
        .withColumn("event_date", StringToDate(concat($"date",$"time")))
        .filter(($"deviceIdUniqe" isin (devicesForThisCity: _*)) && $"event_date">predictionStart && $"event_date"<predictionEnd)

      //results.show()
      //println("count=" + results.count())

      if (FileSystem.get(new URI("s3n://magnet-fwm"), sc.hadoopConfiguration).exists(new Path(results_path))) {
        println("Deleting " + results_path)
        FileSystem.get(new URI("s3n://magnet-fwm"), sc.hadoopConfiguration).delete(new Path(results_path), true)
      }
      println("saving results ...")
      results.select(
        $"deviceIdUniqe",
        $"event_date",
        $"type",
        $"value",
        $"name",
        $"event_id").write
        .format("com.databricks.spark.csv")
        .option("codec", "org.apache.hadoop.io.compress.GzipCodec")
        .save(results_path)
    }

    //</editor-fold>

    println("done!")
  }
}

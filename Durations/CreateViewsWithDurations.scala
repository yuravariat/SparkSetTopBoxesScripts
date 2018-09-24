package SetTopBoxes.Durations

import java.text.SimpleDateFormat
import java.util.concurrent.TimeUnit

import org.apache.hadoop.io.compress.GzipCodec
import org.apache.spark.sql.SparkSession
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

/**
  * Created by yurav on 19/04/2017.
  * spark-shell --driver-memory 10G --executor-memory 18G -i test.scala
  * nohup spark-shell --driver-memory 10G --executor-memory 15G -i test.scala >> out.log &
  */
object CreateViewsWithDurations {

  val spark: SparkSession = SparkSession
    .builder()
    .master("local")
    .appName("Scala Spark SQL")
    .config("spark.sql.warehouse.dir", "file:///tmp/spark-warehouse")
    .getOrCreate()

  sealed trait LogType

  case object VIEW extends LogType

  case object LOG extends LogType

  case class ViewingData(
                          device_id_unique: String,
                          log_type: Int, // 0 - view, 1- log
                          event_date_time: java.sql.Timestamp,
                          station_num: String,
                          prog_code: String,
                          var duration: Int,
                          prog_duration: Int
                        ) extends Serializable

  /** Main function */
  def main(args: Array[String]): Unit = {
    @transient val conf: SparkConf = new SparkConf().setMaster("local").setAppName("Devices")
    @transient val sc: SparkContext = new SparkContext(conf)

    import spark.implicits._

    val format = new SimpleDateFormat("yyyyMMddHHmmss")

    val views_with_logs_path = "s3n://magnet-fwm/home/TsviKuflik/y_views_logs_union/part*.gz"
    val results_path = "s3n://magnet-fwm/home/TsviKuflik/y_views_with_durations"

    val allviews = spark.read
      .format("com.databricks.spark.csv")
      .option("delimiter", "|")
      .load(views_with_logs_path)
    //.where($"_c0" === "01540-000000033234")

    val grouped = allviews.map(v => {
      val dt = new java.sql.Timestamp(format.parse(v.getString(2)).getTime)
      ViewingData(v.getString(0), v.getString(1).toInt, dt, v.getString(3), v.getString(4), 0, v.getString(5).toInt)
    })
      .groupByKey(v => v.device_id_unique)
      .mapGroups((device_id, views) => {
        val newViews = views.toArray.sortBy(v => v.event_date_time.getTime)
        var prevView: ViewingData = null
        for (i <- newViews.indices) {
          if (prevView == null && newViews(i).log_type == 0) {
            prevView = newViews(i)
          }
          // OFF event or next view
          else if (prevView != null &&
            (newViews(i).log_type == 0 || (newViews(i).log_type == 1 && newViews(i).prog_code == "OFF"))) {

            val diff = newViews(i).event_date_time.getTime - prevView.event_date_time.getTime
            val totalMinutes = TimeUnit.MILLISECONDS.toMinutes(diff).toInt
            prevView.duration = totalMinutes
            if (prevView.prog_duration < prevView.duration) {
              prevView.duration = prevView.prog_duration
            }
            if (newViews(i).log_type == 1) {
              prevView = null
            }
            else {
              prevView = newViews(i)
            }
          }
        }
        (device_id, newViews.filter(v => v.log_type == 0))
      })
      .flatMap(g => g._2)
      .drop($"log_type")

    grouped.write
      .format("com.databricks.spark.csv")
      .option("codec", "org.apache.hadoop.io.compress.GzipCodec")
      .save(results_path)

    print("done!")
  }
}

package SetTopBoxes.Programs

import org.apache.spark.sql.SparkSession

/**
  * Created by yurav on 19/04/2017.
  */
object ProgramsRates {
  import org.apache.spark._

  case class ViewingData(mso_code:String,device_id:String,event_date:String,event_time:String,station_num:String,prog_code:String) extends Serializable

  @transient val conf: SparkConf = new SparkConf().setMaster("local").setAppName("Devices")
  @transient val sc: SparkContext = new SparkContext(conf)
  @transient val spark:SparkSession = SparkSession
    .builder()
    .master("local")
    .appName("Java Spark SQL Example")
    .config("spark.sql.warehouse.dir", "file:///tmp/spark-warehouse")
    .getOrCreate();

  /** Main function */
  def main(args: Array[String]): Unit = {

    val views_lines = sc.textFile("F:\\SintecMediaData\\rpt_prog_view\\*.pd.gz")
    val views = views_lines.map(line => {
      val arr = line.split("\\|")
      ViewingData(
        mso_code=arr(0).trim,
        device_id=arr(1).trim,
        event_date=arr(2).trim,
        event_time= arr(3).trim,
        station_num=arr(4).trim,
        prog_code=arr(5).trim
      )
    })

    val programs_rates = views
      .map(v=>(v.prog_code,1))
      .reduceByKey(_ + _)
      .collect()
      .sortBy(-_._2)

    val df = spark.createDataFrame(programs_rates)
    df.coalesce(1)
      .write
      .format("com.databricks.spark.csv")
      .save("D:\\SintecMediaData\\playground\\programs_rates")
  }
}

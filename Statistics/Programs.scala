package SetTopBoxes.Statistics

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by yurav on 19/04/2017.
  */
object Programs {
  @transient val conf: SparkConf = new SparkConf().setMaster("local").setAppName("Devices")
  @transient val sc: SparkContext = new SparkContext(conf)

  val spark: SparkSession = SparkSession
    .builder()
    .master("local")
    .appName("Scala Spark SQL")
    .config("spark.sql.warehouse.dir", "file:///tmp/spark-warehouse")
    .getOrCreate()

  /** Main function */
  def main(args: Array[String]): Unit = {

    sc.getConf.set("fs.s3n.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
    sc.getConf.set("fs.s3n.awsAccessKeyId", "")
    sc.getConf.set("fs.s3n.awsSecretAccessKey", "")

    //<editor-fold desc="Run area">

    import org.apache.spark.sql.functions._
    import spark.implicits._

    println("\n" + "#" * 70 + " Statistics programs " + "#" * 70 + "\n")

    val programs = sc.textFile("s3n://magnet-fwm/home/TsviKuflik/rpt_programs/SintecMedia.rpt_programs*.gz")
      .map(s => {
        val sp = s.split("\\|")
        val prog_id = sp(0)
        val name = sp(1)
        (name, scala.collection.mutable.ListBuffer[String](prog_id))
      })
      .reduceByKey((a, b) => {
        b.foreach(s => {
          if (a.indexOf(s) < 0) {
            a.append(s)
          }
        })
        a
      })
      .sortBy(d => d._1)

    println("total distint programs: " + programs.count())



    println("done!")

    //</editor-fold>
  }
}

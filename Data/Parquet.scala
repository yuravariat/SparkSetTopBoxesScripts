package SetTopBoxes.Data

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SparkSession, _}

object Parquet {

  @transient val spark:SparkSession = SparkSession
    .builder()
    .master("local")
    .appName("Scala Spark SQL")
    .config("spark.sql.warehouse.dir", "file:///tmp/spark-warehouse")
    .getOrCreate();

  def main(args: Array[String]): Unit = {

    import spark.implicits._
    val filePath = "D:\\SintecMediaData\\y_for_clustering\\devices_120_hist_Abilene-Sweetwater_progs_vect_1_results\\data\\part*"

    val parquetDF = spark.read
      .parquet(filePath)
      //.agg(first($"point"))
      //.where(!isnull($"InspectedDate"))

    parquetDF.show()
    parquetDF
      .coalesce(1)
      .write
      .json("D:\\SintecMediaData\\y_for_clustering\\devices_120_hist_Abilene-Sweetwater_progs_vect_1_results\\json")
  }
}

package SetTopBoxes.Devices

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{SparkSession, _}

/**
  * Created by yurav on 19/04/2017.
  */
object Flat_Devices {
  val spark:SparkSession = SparkSession
    .builder()
    .master("local")
    .appName("Java Spark SQL Example")
    //.config("spark.sql.warehouse.dir", "file:///tmp/spark-warehouse")
    .getOrCreate();

  import spark.implicits._

  /** Main function */
  def main(args: Array[String]): Unit = {

    val customSchema = StructType(Array(
      StructField("_mso", StringType, nullable = true),
      StructField("_mso-name", StringType, nullable = true),
      StructField("mapping", ArrayType(StructType(Array(
        StructField("_device-id", StringType),
        StructField("_dma", StringType),
        StructField("_dma-code", StringType),
        StructField("_household-id", StringType),
        StructField("_household-type", StringType),
        StructField("_system-type", StringType),
        StructField("_zipcode", StringType)
      ))))))


    //val files = FileSystem.get(new Configuration())
    //  .listStatus( new Path("D:\\SintecMediaData\\refxml\\"))
    //.foreach( x => println(x.getPath ))

    val files = FileSystem.get(new java.net.URI("s3n://magnet-fwm/home/TsviKuflik/refxml/"),new Configuration())
      .listStatus( new Path("s3n://magnet-fwm/home/TsviKuflik/refxml/"))
      //.foreach( x => println(x.getPath ))

    files.foreach { case (x)=>
      if (x.getPath.toString.contains("SintecMedia.rpt_refxml.date_2015-01-07.2016-11-21.xml.gz")) {

        //val date = x.getPath.toString.replace("file:/D:/SintecMediaData/refxml/SintecMedia.rpt_refxml.date_","").substring(0,10)
        val date = x.getPath.toString.replace("s3n://magnet-fwm/home/TsviKuflik/refxml/SintecMedia.rpt_refxml.date_","").substring(0,10)

        val HouseHoldsDF:DataFrame = spark.read
          .format("com.databricks.spark.xml")
          .option("rootTag","fwm:reference")
          .option("rowTag","mso-table")
          .schema(customSchema)
          .load(x.getPath.toString)

        HouseHoldsDF.printSchema()
        HouseHoldsDF.show(5)

        HouseHoldsDF.createOrReplaceTempView("HouseHoldsXML")

        val FlattenHouseHoldsDF = HouseHoldsDF
          .withColumn("date", when($"_mso"==="",date).otherwise(date))
          .withColumn("mapping_item", explode(col("mapping")))
          .select(
            $"date",
            $"_mso" as "mso",
            $"_mso-name" as "mso-name",
            $"mapping_item._device-id" as "device-id",
            $"mapping_item._dma" as "city",
            $"mapping_item._dma-code" as "city-code",
            $"mapping_item._household-id" as "household-id",
            $"mapping_item._household-type" as "household-type",
            $"mapping_item._system-type" as "system-type",
            $"mapping_item._zipcode" as "zipcode"
          )
        FlattenHouseHoldsDF.show()

//        FlattenHouseHoldsDF
//          // place all data in a single partition
//          //.coalesce(1)
//          .write.format("com.databricks.spark.csv")
//          .option("header", "true")
//          .save("F:\\SintecMediaData\\refxml\\1-smal-sample.csv")
      }
    }

    val rr =3
  }
}

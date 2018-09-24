package SetTopBoxes.Genres

/**
  * Created by yurav on 19/04/2017.
  */
object Genres {
  import org.apache.spark._

  @transient val conf: SparkConf = new SparkConf().setMaster("local").setAppName("Devices")
  @transient val sc: SparkContext = new SparkContext(conf)

  /** Main function */
  def main(args: Array[String]): Unit = {



    val programs = sc.textFile("s3n://magnet-fwm/home/TsviKuflik/rpt_programs/*.gz")

    //Genres
    val genres = programs
      .flatMap{case (line) =>
        val genresStr = line.split('|').lift(2)
        if(genresStr.isDefined){
          genresStr.get.split(',')
        }
        else{
          "".split(',')
        }
      }
      .map(genre => (genre, 1))
      .reduceByKey(_ + _)
      //.collect()
      .sortBy(_._1)

    genres.map(p => p._1 + "|" + p._2)
      .coalesce(1)
      .saveAsTextFile("s3n://magnet-fwm/home/TsviKuflik/y_programs/genres")


    val counts = programs
      .map(line => {
        if (!line.isEmpty()) {
          val genresStr = line.split('|')
          if(genresStr.length>4 ) { //&& genresStr(4).endsWith("0000")
            (genresStr(3) + "|" + genresStr(4), 1)
          }
          else{
            ("",0)
          }
        }
        else{
          ("",0)
        }
      })
      .reduceByKey(_ + _)
      .collect()

//    counts.foreach(c=>
//      println(c._1 + " -> " + c._2 )
//    )

    println("\nTotal " + counts.length + " progsTimes")
    val average = counts.map(p=>p._2).sum / counts.length
    println("\nAverage " + average)
    println("\nMax " + counts.map(p=>p._2).max)
  }
}

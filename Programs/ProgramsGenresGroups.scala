package SetTopBoxes.Programs

/**
  * Created by yurav on 19/04/2017.
  */
object ProgramsGenresGroups {

  import org.apache.spark._

  @transient val conf: SparkConf = new SparkConf().setMaster("local").setAppName("Devices")
  @transient val sc: SparkContext = new SparkContext(conf)

  /** Main function */
  def main(args: Array[String]): Unit = {

    //<editor-fold desc="Run area">

    println("\n"+"#" * 70 + " ProgramsGenresGroups " + "#" * 70 + "\n")

    val programs = sc.textFile("s3n://magnet-fwm/home/TsviKuflik/rpt_programs/SintecMedia.rpt_programs.date_2015-01*.gz," +
      "s3n://magnet-fwm/home/TsviKuflik/rpt_programs/SintecMedia.rpt_programs.date_2015-02*.gz," +
      "s3n://magnet-fwm/home/TsviKuflik/rpt_programs/SintecMedia.rpt_programs.date_2015-03*.gz," +
      "s3n://magnet-fwm/home/TsviKuflik/rpt_programs/SintecMedia.rpt_programs.date_2015-04*.gz," +
      "s3n://magnet-fwm/home/TsviKuflik/rpt_programs/SintecMedia.rpt_programs.date_2015-05*.gz," +
      "s3n://magnet-fwm/home/TsviKuflik/rpt_programs/SintecMedia.rpt_programs.date_2015-06*.gz," +
      "s3n://magnet-fwm/home/TsviKuflik/rpt_programs/SintecMedia.rpt_programs.date_2015-07*.gz")
      .map(s => {
        val sp = s.split("\\|")
        val prog_id = sp(0)
        val name = sp(1)
        val genres = sp(2)
        (name, (genres, scala.collection.mutable.ListBuffer[String](prog_id)))
      })
      .reduceByKey((a, b) => {
        b._2.foreach(s => {
          if (a._2.indexOf(s) < 0) {
            a._2.append(s)
          }
        })
        a
      })
      .sortBy(d => d._1)

    programs.map(p => p._1 + "|" + p._2._1 + "|" + p._2._2.mkString("|"))
      .coalesce(1)
      .saveAsTextFile("s3n://magnet-fwm/home/TsviKuflik/y_programs/programs_genres_groups")

    print("done!")

    //</editor-fold>

  }
}

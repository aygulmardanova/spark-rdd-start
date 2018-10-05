import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

/*
*
* a) Give the total count of each hero mentioned in this data;
* b) Give the total count of killed enemies for each of hero.
*
*/
object SuperHeroes {

  val def_filename = "heroes.txt"

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    conf.setAppName("Heroes")
    conf.setMaster("local[2]")

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val sc = new SparkContext(conf)

    val fileName = if (args.isEmpty) def_filename else args(0)

    val fileRDD = sc.textFile(fileName)
    fileRDD.foreach(println)

    val rows = fileRDD.map(_.split(";"))

    println("\nCount of each hero:")
    rows
      .map(hero => (hero(1), 1)).
      reduceByKey(_ + _)
      .foreach(mention => println(mention._1 + ": " + mention._2))

    println("\nCount of killed enemies:")
    rows
      .map(hero => (hero(1), hero(2).toInt))
      .reduceByKey(_ + _)
      .foreach(killed => println(killed._1 + ": " + killed._2))
  }
}

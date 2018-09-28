import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object RDDOperations {

  val def_filename = "input.txt"
  val take_num_count = 20
  val outputDir = "output"
  val partedResult = "/parted"
  val oneFileResult = "/oneFile"

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    conf.setAppName("RDD Operations")
    conf.setMaster("local[2]")

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val sc = new SparkContext(conf)

    val fileName = if (args.isEmpty) def_filename else args(0)

    println("File " + fileName + ":")
    val fileRDD = sc.textFile(fileName)
    fileRDD.foreach(println)

    val rows = fileRDD.map(_.split(" "))
    // create the RDD of numbers
    val numbers = rows.map(_.map(_.toInt))

    println("RDD[Array[Int]]")
    println("Array of numbers")
    numbers.map(_.mkString("", " ", "\n")).takeOrdered(take_num_count).foreach(print)

    println("\nSum of numbers of each row:")
    val rowsSum = numbers.map(_.sum.toString)
    rowsSum.foreach(println)

    println("\nSum of numbers for each row (number %5 == 0):")
    val rowsMult5Sum = numbers.map(_.filter(_ % 5 == 0).sum.toString)
    rowsMult5Sum.foreach(println)

    println("\nMaximum for each row:")
    val rowsMax = numbers.map(_.max.toString)
    rowsMax.foreach(println)

    println("\nMinimum for each row:")
    val rowsMin = numbers.map(_.min.toString)
    rowsMin.foreach(println)

    println("\nSet of numbers for each row:")
    val rowsSet = numbers.map(_.distinct)
    rowsSet.map(_.mkString("", " ", "\n")).takeOrdered(take_num_count).foreach(print)

    println("\nRDD[Int]")
    println("Get an array of all numbers:")
    var intFlat = fileRDD.flatMap(_.split(" ")).map(_.toInt)
    intFlat.map(_ + " ").foreach(print)

    println("\nSum of all numbers:")
    var flatSum = intFlat.reduce(_ + _).toString
    println(flatSum)

    println("\nSum of all numbers % 5 == 0:")
    val flatMult5Sum = intFlat.filter(_ % 5 == 0).reduce(_ + _).toString
    println(flatMult5Sum)

    println("\nMaximum of all numbers:")
    val flatMax = intFlat.max.toString
    println(flatMax)

    println("\nMinimum of all numbers:")
    val flatMin = intFlat.min.toString
    println(flatMin)

    println("\nSet of all numbers:")
    val intFlatSet = intFlat.distinct()
    intFlatSet.takeOrdered(take_num_count).mkString("", " ", "\n").foreach(print)

    val result =
      sc.parallelize(Seq("File " + fileName + ":\n")) ++ fileRDD ++
        sc.parallelize(Seq("\nSum of numbers of each row:\n")) ++ rowsSum ++
        sc.parallelize(Seq("\nSum of numbers for each row (number %5 == 0):\n")) ++ rowsMult5Sum ++
        sc.parallelize(Seq("\nMaximum for each row:\n")) ++ rowsMax ++
        sc.parallelize(Seq("\nMinimum for each row:\n")) ++ rowsMin ++
        sc.parallelize(Seq("\nSet of numbers for each row:\n")) ++ rowsSet.map(_.mkString("", " ", "")) ++
        sc.parallelize(Seq("\nGet an array of all numbers:\n")) ++ intFlat.map(_ + " ") ++
        sc.parallelize(Seq("\nSum of all numbers:\n" + flatSum)) ++
        sc.parallelize(Seq("\nSum of all numbers % 5 == 0:\n" + flatMult5Sum)) ++
        sc.parallelize(Seq("\nMaximum of all numbers:\n" + flatMax)) ++
        sc.parallelize(Seq("\nMinimum of all numbers:\n" + flatMin)) ++
        sc.parallelize(Seq("\nSet of all numbers:\n")) ++ intFlatSet.map(_ + " ")

    val fs = FileSystem.get(sc.hadoopConfiguration)
    fs.delete(new Path(outputDir), true)
    result.saveAsTextFile(outputDir + partedResult)
    result.coalesce(1).saveAsTextFile(outputDir + oneFileResult)
  }

}
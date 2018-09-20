import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object RDDOperations {

  val take_num_count = 20

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    conf.setAppName("RDD Operations")
    conf.setMaster("local[2]")

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val sc = new SparkContext(conf)

    val fileName = if (args.isEmpty) "input.txt" else args(0)

    println("File " + fileName + ":")
    val fileRDD = sc.textFile(fileName)
    fileRDD.foreach(println)

    val rows = fileRDD.map(line => line.split(" "))
    // create the RDD of numbers
    val numbers = rows.map(row => row.map(s => s.toInt))

    println("RDD[Array[Int]]")
    println("Array of numbers")
    numbers.map(number => number.mkString("", " ", "\n")).takeOrdered(take_num_count).foreach(print)

    println("\nSum of numbers of each row:")
//    val rowsSum = numbers.map(number => number.reduce((a, b) => a + b))
    val rowsSum = numbers.map(number => number.sum)
    rowsSum.foreach(println)

    println("\nSum of numbers for each row (number %5 == 0):")
    val rowsMult5Sum = numbers.map(number => number.filter(num => num % 5 == 0).sum)
    rowsMult5Sum.foreach(println)

    println("\nMaximum for each row:")
    val rowsMax = numbers.map(number => number.max)
    rowsMax.foreach(println)
//    val rowsMax = numbers.map(number => number.reduce((a, b) => if (a >= b) a else b))
//    rowsMax.map(max => max.toString + " ").takeOrdered(take_num_count).foreach(print)

    println("\nMinimum for each row:")
    val rowsMin = numbers.map(number => number.min)
    rowsMin.foreach(println)

    println("\nSet of numbers for each row:")
    val rowsSet = numbers.map(numbersRow => numbersRow.distinct)
    rowsSet.map(set => set.mkString("", " ", "\n")).takeOrdered(take_num_count).foreach(print)

    println("\nRDD[Int]")
    println("Get an array of all numbers:")
    var intFlat = fileRDD.flatMap(s => s.split(" ")).map(s => s.toInt)
    intFlat.map(int => int.toString + " ").foreach(print)

    println("\nSum of all numbers:")
    var flatSum = intFlat.reduce((a, b) => a + b)
    println(flatSum)

    println("\nSum of all numbers % 5 == 0:")
    val flatMult5Sum = intFlat.filter(d => d % 5 == 0).reduce((a, b) => a + b)
    println(flatMult5Sum)

    println("\nMaximum of all numbers:")
    val flatMax = intFlat.max
    println(flatMax)

    println("\nMinimum of all numbers:")
    val flatMin = intFlat.min
    println(flatMin)

    println("\nSet of all numbers:")
    val intFlatSet = intFlat.distinct()
    intFlatSet.takeOrdered(take_num_count).mkString("", " ", "\n").foreach(print)

  }

}
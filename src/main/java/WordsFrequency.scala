import org.apache.spark.{SparkConf, SparkContext}
import org.apache.log4j.Logger
import org.apache.log4j.Level

object WordsFrequency {

    val def_filename = "src/main/resources/product.csv"

    def main(args: Array[String]): Unit = {

        val conf = new SparkConf()
        conf.setAppName("WordsFrequency")
        conf.setMaster("local[2]")

        Logger.getLogger("org").setLevel(Level.OFF)
        Logger.getLogger("akka").setLevel(Level.OFF)

        val sc = new SparkContext(conf)

        val fileName = if (args.isEmpty) def_filename else args(0)

        val fileRDD = sc.textFile(fileName)
        val words = fileRDD.flatMap(_.split(" "))

//        each word present as a tuple with initial count 1 (word, 1)
//        then reduce by key and calculate the total count for each word
        val wordCount = words
          .map(x => (x, 1))
          .reduceByKey((a, b) => a + b)

        // Word amount
        val wordsCount = words.count().toFloat

        val wordFreq = wordCount
          .map(x => (x._1, x._2, x._2.toFloat / wordsCount))

        wordFreq.foreach(println)

    }
}
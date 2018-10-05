import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Dataset, SQLContext, SparkSession}

object SQLOperations {

  val def_filename = "src/main/resources/sampletweets.json"

  def main(args: Array[String]): Unit = {
    val jsonFileName = if (args.isEmpty) def_filename else args(0)

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    //Initialize SparkSession
    val sparkSession = SparkSession
      .builder()
      .appName("spark-sql-tweets")
      .master("local[*]")
      .getOrCreate()

    //Read json file to DF
    val tweetsDF = sparkSession.read.json(jsonFileName)

    //Show the first 100 rows
    tweetsDF.show(100)

    //Show the scheme of DF
    tweetsDF.printSchema()

    // Register the DataFrame as a SQL temporary view
    tweetsDF.createOrReplaceTempView("tweetTable")

   // Read all tweets from input files (extract the message from the tweets)
    val tweetsMessages = sparkSession
      .sql("SELECT actor.displayName, body " +
        "FROM tweetTable " +
        "WHERE body IS NOT NULL " +
        "   AND object.twitter_lang = 'ru'")
    tweetsMessages.show(100)

    // Count the most active languages
    val activeLanguages = sparkSession
      .sql("SELECT actor.languages, COUNT(*) as count " +
        "FROM tweetTable " +
        "GROUP BY actor.languages " +
        "ORDER BY count DESC LIMIT 25")
    activeLanguages.show(25)

  }
}

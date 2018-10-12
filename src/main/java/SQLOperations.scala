import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.to_timestamp
import org.apache.spark.sql.functions.explode
import org.apache.spark.sql.functions.when
import org.apache.spark.sql.functions.size
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{SparkSession}

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

        import sparkSession.implicits._

        //Read json file to DF
        val tweetsDF = sparkSession
          .read
          .json(jsonFileName)
          .withColumn("postedTime",
              to_timestamp($"postedTime", "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"))
//          .withColumn("twitter_entities.user_mentions", when(size($"twitter_entities.user_mentions") === 0, lit(None)).otherwise(col("arr")))
        //Show the first 10 rows
        val trr = tweetsDF.schema.filter(_.dataType.typeName == "array").map(_.name).foldLeft(tweetsDF)((acc, colname) => acc.withColumn(colname,
            when(size(col(colname)) === 0, null).otherwise(col(colname))))
        tweetsDF.show(10)

        //Show the scheme of DF
        tweetsDF.printSchema()

        // Register the DataFrame as a SQL temporary view
        tweetsDF.createOrReplaceTempView("tweetTable")
        trr.createOrReplaceTempView("trrTable")

        // Read all tweets from input files (extract the message from the tweets)
        val tweetsMessages = sparkSession
          .sql("SELECT actor.displayName AS author, body AS tweet " +
            "FROM tweetTable " +
            "WHERE body IS NOT NULL " +
            "   AND object.twitter_lang = 'ru'")
        tweetsMessages.show()

        // Count the most active languages
        val activeLanguages = sparkSession
          .sql("SELECT actor.languages AS language, COUNT(*) AS count " +
            "FROM tweetTable " +
            "WHERE actor.languages IS NOT NULL " +
            "GROUP BY actor.languages " +
            "ORDER BY count DESC LIMIT 25")
        activeLanguages.show()

        // Get latest tweet date
        val maxDate = sparkSession
          .sql("SELECT actor.displayName AS author, postedTime AS latest, body AS tweet " +
            "FROM tweetTable " +
            "ORDER BY latest DESC NULLS LAST LIMIT 1")
        maxDate.show()

        // Get earliest tweet date
        val minDate = sparkSession
          .sql("SELECT actor.displayName AS author, postedTime AS earliest, body AS tweet " +
            "FROM tweetTable " +
            "ORDER BY earliest ASC NULLS LAST LIMIT 1")
        minDate.show()

        // Get Top devices used among all Twitter users
        val devices = sparkSession
          .sql("SELECT generator.displayName AS device, COUNT(*) AS count " +
            "FROM tweetTable " +
            "WHERE generator.displayName IS NOT NULL " +
            "GROUP BY generator.displayName " +
            "ORDER BY count DESC NULLS LAST LIMIT 25")
        devices.show()

        // Find count of tweets for the most active users
        val userTwsCount = sparkSession
          .sql("SELECT actor.displayName AS author, COUNT(*) AS count " +
            "FROM tweetTable " +
            "WHERE actor.displayName IS NOT NULL " +
            "GROUP BY actor.displayName " +
            "ORDER BY count DESC LIMIT 5")
        userTwsCount.show()

        // Find all the tweets by user PLEASE AARON
        val userTws = sparkSession
          .sql("SELECT actor.displayName AS author, body AS tweet " +
            "FROM tweetTable " +
            "WHERE actor.displayName = 'PLEASE AARON' AND body IS NOT NULL")
        userTws.show(10)

        //Top users who has a lot of friends
        sparkSession.sql(
            " SELECT actor.displayName, actor.friendsCount " +
              "FROM tweetTable " +
              "ORDER BY actor.friendsCount DESC LIMIT 25")
          .show()

        // Find persons mentions for each tweet
        val userMents = sparkSession
          .sql("SELECT twitter_entities.user_mentions.name AS mentionedUsers, body " +
            "FROM tweetTable " +
            "WHERE twitter_entities.user_mentions IS NOT NULL " +
            "   AND size(twitter_entities.user_mentions) > 0 ")
        userMents.show(5)

        // Find all the persons mentioned on tweets, order by mentions count, 10 first
        val userMentions = sparkSession
          .sql("SELECT twitter_entities.user_mentions " +
            "FROM tweetTable " +
            "WHERE twitter_entities.user_mentions IS NOT NULL " +
            "   AND size(twitter_entities.user_mentions) > 0 ")
        userMentions.select(explode($"user_mentions").as("userMentions")).toDF()
        userMentions.createOrReplaceTempView("userMentions")
        userMentions.printSchema()
        sparkSession.sql("SELECT DISTINCT user_mentions.name AS name " +
          "FROM userMentions ").show(25)

        // Count how many times each person is mentioned
        // Find the 10 most mentioned persons
        val userMentionsCount = sparkSession.sql("SELECT user_mentions.name AS name, COUNT(*) AS count " +
          "FROM userMentions " +
          "GROUP BY name ORDER BY count DESC ")
        userMentionsCount.show(10)

        // All the mentioned hashtags
        val hashTagMentions = sparkSession
          .sql("SELECT twitter_entities.hashtags " +
            "FROM tweetTable " +
            "WHERE twitter_entities.hashtags IS NOT NULL " +
            "   AND size(twitter_entities.hashtags) > 0 ")
        hashTagMentions.select(explode($"hashtags").as("hashTags")).toDF()
        hashTagMentions.createOrReplaceTempView("hashTags")
        hashTagMentions.printSchema()
        sparkSession.sql("SELECT DISTINCT hashTags.text AS hashTag, hashTags.indices as indices " +
          "FROM hashTags ").show(25)

        // Count of each hashtag being mentioned
        // Find 10 most popular hashtags
        val hashTagMentionsCount = sparkSession.sql("SELECT hashTags.text AS hashTag, COUNT(*) AS count " +
          "FROM hashTags " +
          "GROUP BY hashTag ORDER BY count DESC ")
        hashTagMentionsCount.show(10)

    }
}

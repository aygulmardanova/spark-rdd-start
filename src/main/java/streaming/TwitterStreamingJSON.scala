package streaming

import com.google.gson.Gson
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.twitter.TwitterUtils

object TwitterStreamingJSON {

    def main(args: Array[String]): Unit = {

        Logger.getLogger("org").setLevel(Level.OFF)
        Logger.getLogger("akka").setLevel(Level.OFF)

        val conf = new SparkConf()
        conf.setAppName("language-classifier")
        conf.setMaster("local[*]")

        val sc = new SparkContext(conf)
        val ssc = new StreamingContext(sc, Seconds(30))

        // Configure Twitter credentials
        TwitterStreamingUtils.initTwitterStream()

        // Create Twitter Stream in JSON
        val tweets = TwitterUtils
          .createStream(ssc, None)
          .map(new Gson().toJson(_))

        val numTweetsCollect = 10000L
        var numTweetsCollected = 0L

        //Save tweets in file
        tweets.foreachRDD((rdd, time) => {
            val count = rdd.count()
            if (count > 0) {
                val outputRDD = rdd.coalesce(1)
                outputRDD.saveAsTextFile(TwitterStreamingUtils.twitter_streaming_json_output + time)
                numTweetsCollected += count
                if (numTweetsCollected > numTweetsCollect) {
                    System.exit(0)
                }
            }
        })


        ssc.start()
        ssc.awaitTermination()
    }
}
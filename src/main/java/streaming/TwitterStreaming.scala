package streaming

import org.apache.log4j.{Level, Logger}
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object TwitterStreaming {

    def main(args: Array[String]): Unit = {

        Logger.getLogger("org").setLevel(Level.OFF)
        Logger.getLogger("akka").setLevel(Level.OFF)

        // Configure Twitter credentials
        TwitterStreamingUtils.initTwitterStream()

        val conf = new SparkConf()
        conf.setAppName("spark-streaming")
        conf.setMaster("local[2]")
        val sc = new SparkContext(conf)

        val ssc = new StreamingContext(sc, Seconds(1))

        // Create Twitter Stream
        val stream = TwitterUtils.createStream(ssc, None)
        val tweets = stream.map(t => t.getText)
        tweets.print()

        tweets.saveAsTextFiles(TwitterStreamingUtils.twitter_streaming_output)
        //get hash tag, calculate the hash tags count
        val hashTags = tweets.flatMap(_.split(" ")).filter(_.startsWith("#"))
        val result = hashTags.map(hashTag => (hashTag, 1)).reduceByKey(_ + _)

        hashTags.print()
        result.print(10)

        ssc.start()
        ssc.awaitTermination()
    }
}
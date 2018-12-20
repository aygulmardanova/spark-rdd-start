package streaming

import org.apache.log4j.{Level, Logger}
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object TwitterStreamingExample {
    def main(args: Array[String]): Unit = {

        Logger.getLogger("org").setLevel(Level.OFF)
        Logger.getLogger("akka").setLevel(Level.OFF)

        val conf = new SparkConf()
        conf.setAppName("spark-streaming")
        conf.setMaster("local[2]")
        val sc = new SparkContext(conf)

        val ssc = new StreamingContext(sc, Seconds(1))

        // Configure Twitter credentials
        TwitterStreamingUtils.initTwitterStream()

        // Create Twitter Stream
        val stream = TwitterUtils.createStream(ssc, None)
        val tweets = stream.map(_.getText)

        tweets.print()

        ssc.start()
        ssc.awaitTermination()
    }
}


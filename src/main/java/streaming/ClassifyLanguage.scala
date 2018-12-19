package streaming

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.clustering.KMeansModel
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object ClassifyLanguage {

    def main(args: Array[String]): Unit = {

        Logger.getLogger("org").setLevel(Level.OFF)
        Logger.getLogger("akka").setLevel(Level.OFF)

        val modelInput = TwitterStreamingUtils.twitter_streaming_model_output

        val conf = new SparkConf()
        conf.setAppName("language-classifier")
        conf.setMaster("local[*]")

        val sc = new SparkContext(conf)
        val ssc = new StreamingContext(sc, Seconds(5))

        TwitterStreamingUtils.initTwitterStream()

        // Create Twitter Stream
        println("Initializing Twitter stream")
        val tweets = TwitterUtils
          .createStream(ssc, None)

        val texts = tweets.map(_.getText)

        println("Initializing the KMeans model")
        val model = KMeansModel
          .load(sc, modelInput)
        val langNumber = 3

        val filtered = texts
          .filter(t => model.predict(TwitterStreamingUtils.featurize(t)) == langNumber)
        filtered.print()

        ssc.start()
        ssc.awaitTermination()
    }
}
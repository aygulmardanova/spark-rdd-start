package streaming

import org.apache.spark.mllib.feature.HashingTF

object TwitterStreamingUtils {

    val twitter_streaming_output = "output/stream/tweets"
    val twitter_streaming_json_output = "output/stream-json/tweets"
    val twitter_streaming_model_output = "output/stream-model/tweets"

    def initTwitterStream(): Unit = {

        val apiKey = "KNVTwiYajvHab5NBqBWSWepde"
        val apiSecret = "62eWYe5bBZlf09mXzpdgtOAzSBm7FxlkoZCgCYmRblpTOSI4wu"
        val accessToken = "863078539347861504-eWeovitnbZ2g8eNinenoMTz1PTfMbOY"
        val accessTokenSecret = "UP1tsTaFuVXTmDJ3nIxGYo0mhqt3ybOuClEQ1V4oTWvbT"

        System.setProperty("twitter4j.oauth.consumerKey", apiKey)
        System.setProperty("twitter4j.oauth.consumerSecret", apiSecret)
        System.setProperty("twitter4j.oauth.accessToken", accessToken)
        System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret)
    }

    //Featurize Function
    def featurize(s: String) = {
        val numFeatures = 1000
        val tf = new HashingTF(numFeatures)
        tf.transform(s.sliding(2).toSeq)
    }

}

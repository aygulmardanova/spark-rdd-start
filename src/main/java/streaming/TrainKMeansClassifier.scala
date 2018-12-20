package streaming

import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.sql.SparkSession

object TrainKMeansClassifier {

    def main(args: Array[String]): Unit = {

        Logger.getLogger("org").setLevel(Level.OFF)
        Logger.getLogger("akka").setLevel(Level.OFF)

        val jsonFile = TwitterStreamingUtils.twitter_streaming_json_output + "[0-9]* ms/part-00000"
        val modelOutput = TwitterStreamingUtils.twitter_streaming_model_output

        val sparkSession = SparkSession
          .builder()
          .appName("spark-tweets-train-kmeans")
          .master("local[*]")
          .getOrCreate()

        val tweetsDF = sparkSession.read.json(jsonFile)

        //Show the first 100 rows
        tweetsDF.show(10)

        //Extract the text
        val text = tweetsDF
          .select("text")
          .rdd
          .map(r => r(0).toString)

        //Get the features vector
        val features = text.map(s => TwitterStreamingUtils.featurize(s))

        val numClusters = 10
        val numIterations = 40

        // Train KMeans model and save it to file
        val trainedModel: KMeansModel = KMeans.train(features, numClusters, numIterations)
        trainedModel.save(sparkSession.sparkContext, modelOutput)
    }
}
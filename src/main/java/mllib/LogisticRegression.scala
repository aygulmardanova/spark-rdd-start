package mllib

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.Vectors

object LogisticRegression {

    // csv file without header
    val def_filename = "src/main/resources/diabetes.csv"

    def main(args: Array[String]): Unit = {

        Logger.getLogger("org").setLevel(Level.OFF)
        Logger.getLogger("akka").setLevel(Level.OFF)

        // Initialize spark configurations
        val conf = new SparkConf()
        conf.setAppName("spark-logistic-regression")
        conf.setMaster("local[2]")

        // SparkContext
        val sc = new SparkContext(conf)

        val diabetesFile = if (args.isEmpty) def_filename else args(0)

        val diabetes = sc.textFile(diabetesFile)

        diabetes.take(10).foreach(println)

        // Prepare data for the logistic regression algorithm (get an RDD object)
        val parsedDiabetes = diabetes.map { line =>
            val parts = line.split(",")
            LabeledPoint(parts(8).toDouble, Vectors.dense(parts.slice(0, 8).map(x => x.toDouble)))
        }
        println(parsedDiabetes.take(10).mkString("\n"))

        // Split data into training (60%) and test (40%)
        val diabetesSplits = parsedDiabetes.randomSplit(Array(0.7, 0.3), seed = 11L)

        val trainingData = diabetesSplits(0)
        val testData = diabetesSplits(1)

        // Train the model
        val model = new LogisticRegressionWithLBFGS()
          .setNumClasses(2)
          .run(trainingData)

        // Evaluate model on training examples and compute training error
        val predictionAndLabels = testData.map { point =>
            val prediction = model.predict(point.features)
            (point.label, prediction)
        }

        // Get evaluation metrics
        val metrics = new MulticlassMetrics(predictionAndLabels)
        val accuracy = metrics.accuracy
        println(s"Accuracy = $accuracy")
    }
}
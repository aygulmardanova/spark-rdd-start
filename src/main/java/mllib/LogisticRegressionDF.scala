package mllib

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}
import org.apache.spark.ml.linalg.DenseVector
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.sql.{SparkSession, types}
import org.apache.spark.sql.types._

object LogisticRegressionDF {

    val def_filename = "src/main/resources/diabetesDF.csv"
    var diabetesSchema = StructType(Array(
        StructField("pregnancy", IntegerType, nullable = true),
        StructField("glucose", IntegerType, nullable = true),
        StructField("arterial pressure", IntegerType, nullable = true),
        StructField("thickness of TC", IntegerType, nullable = true),
        StructField("insulin", IntegerType, nullable = true),
        StructField("body mass index", DoubleType, nullable = true),
        StructField("heredity", DoubleType, nullable = true),
        StructField("age", IntegerType, nullable = true),
        StructField("diabetes", IntegerType, nullable = true)))

    def main(args: Array[String]): Unit = {

        Logger.getLogger("org").setLevel(Level.OFF)
        Logger.getLogger("akka").setLevel(Level.OFF)

        val diabetesFile = if (args.isEmpty) def_filename else args(0)

        val sparkSession = SparkSession
          .builder()
          .appName("logistic regression with data frames")
          .master("local[*]")
          .getOrCreate()

        //  load the csv file with predefined structure
        val diabetes = sparkSession.read
          .option("header", "true")
          .option("delimiter", ",")
          .option("nullValue", "")
          .option("treatEmptyValuesAsNulls", "true")
          .schema(diabetesSchema)
          .csv(diabetesFile)
        diabetes.show(10)

        val DFAssembler = new VectorAssembler().
          setInputCols(Array(
              "pregnancy", "glucose", "arterial pressure",
              "thickness of TC", "insulin", "body mass index",
              "heredity", "age")).
          setOutputCol("features")

        val features = DFAssembler.transform(diabetes)
        features.show(10)

        val labeledTransformer = new StringIndexer().setInputCol("diabetes").setOutputCol("label")
        val labeledFeatures = labeledTransformer.fit(features).transform(features)
        labeledFeatures.show(10)

        // Split data into training (60%) and test (40%)
        val diabetesSplits = labeledFeatures.randomSplit(Array(0.7, 0.3), seed = 11L)
        val trainingData = diabetesSplits(0)
        val testData = diabetesSplits(1)

        val logReg = new LogisticRegression()
          .setMaxIter(100)
          .setRegParam(0.3)
          .setElasticNetParam(0.5)

        // Train the model
        val model = logReg.fit(trainingData)

        println(s"Coefficients: ${model.coefficients} Intercept: ${model.intercept}")

        // Make predictions on test data
        val predictions = model.transform(testData)
        predictions.show(10)

        // Evaluate the precision and recall
        val trueCount = predictions.where("label == prediction").count()
        val totalCount = predictions.count()

        println(s"Count of true predictions: $trueCount Total count: $totalCount")

        val evaluator = new BinaryClassificationEvaluator()
          .setLabelCol("label")
          .setRawPredictionCol("rawPrediction")
          .setMetricName("areaUnderROC")

        val accuracy = evaluator.evaluate(predictions)

        println(s"Accuracy = $accuracy")

        val predictionsAndLabels = predictions
          .select("rawPrediction", "label")
          .rdd
          .map(x => (x(0).asInstanceOf[DenseVector](1), x(1).asInstanceOf[Double]))

        val metrics = new BinaryClassificationMetrics(predictionsAndLabels)

        println("Area under the precision-recall curve: " + metrics.areaUnderPR())
        println("Area under the receiver operating characteristic (ROC) curve: " + metrics.areaUnderROC())

    }
}
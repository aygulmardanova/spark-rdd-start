package movielens

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.recommendation.ALS
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}

object MovieLens {

  val directory_name = "src/main/resources/movielens/"
  val ratings_filename = "ratings.csv"
  val movies_filename = "movies.csv"
  val personal_ratings_filename = "personalRatings.csv"

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val sc = new SparkContext("local[2]", "movieLens")

    val ss = SparkSession
      .builder()
      .appName("movieLens")
      .master("local[*]")
      .getOrCreate()

    val ratings = readFile(ss, "ratings", ratings_filename)
    ratings.show(10)
    ratings.printSchema()

    val movies = readFile(ss, "movies", movies_filename)
    movies.show(10)
    movies.printSchema()

    val personalRatings = readFile(ss, "personalRatings", personal_ratings_filename)
    personalRatings.show(10)
    personalRatings.printSchema()

    val ratingsWithPersonal = ratings.union(personalRatings)
    ratingsWithPersonal.show(10)

    // Split dataset into training and testing parts
    val Array(training, test) = ratingsWithPersonal.randomSplit(Array(0.5, 0.5), seed = 11L)

    println("training")
    training.show(10)
    println("test")
    test.show(10)

    val als = new ALS()
      .setMaxIter(3)
      .setRegParam(0.01)
      .setUserCol("user_id")
      .setItemCol("movie_id")
      .setRatingCol("rating")

    //Get trained model
    val model = als.fit(training)

    val predictions = model.transform(test).na.drop

    val evaluator = new RegressionEvaluator()
      .setMetricName("rmse")
      .setLabelCol("rating")
      .setPredictionCol("prediction")
    val rmse = evaluator.evaluate(predictions)

    println(s"Root-mean-square error = $rmse")

    val ratingWithMyRats = ratings.union(ratingsWithPersonal)

    //Get My Predictions
    val myPredictions = model.transform(ratingsWithPersonal).na.drop
  }

  def readFile(ss: SparkSession, schemaName: String, filename: String): DataFrame = {
    ss.read
      .option("header", "true")
      .option("delimiter", ";")
      .option("nullValue", "")
      .option("treatEmptyValuesAsNulls", "true")
//      .option("inferSchema", "true")
                .schema(getSchema(schemaName))
      .csv(directory_name + filename)
      .toDF()
  }

  def getSchema(name: String): StructType = {
    if ("ratings".equals(name) || "personalRatings".equals(name))
      return StructType(Array(
        StructField("user_id", IntegerType, nullable = true),
        StructField("movie_id", IntegerType, nullable = true),
        StructField("rating", IntegerType, nullable = true),
        StructField("timestamp", IntegerType, nullable = true)))
    if ("movies".equals(name))
      return StructType(Array(
        StructField("movie_id", IntegerType, nullable = true),
        StructField("movie_name", StringType, nullable = true),
        StructField("genre", StringType, nullable = true)))
    null
  }

}

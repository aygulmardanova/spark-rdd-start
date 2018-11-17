package movielens

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.{StructField, _}

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

        val movies = readFile(ss, "movies", movies_filename)
        movies.show(10)

        val personalRatings = readFile(ss, "personalRatings", personal_ratings_filename)
        personalRatings.show(10)

        val ratingsWithPersonal = ratings.union(personalRatings)
        ratingsWithPersonal.show(10)

        // Split dataset into training and testing parts
        val Array(training, test) = ratingsWithPersonal.randomSplit(Array(0.5, 0.5), seed = 11L)

        println("training")
        training.show(10)
        println("test")
        test.show(10)

    }

    def readFile(ss: SparkSession, schemaName: String, filename: String): DataFrame = {
        ss.read
          .option("header", "false")
          .option("delimiter", ";")
          .option("nullValue", "")
          .option("treatEmptyValuesAsNulls", "true")
//          .option("inferSchema", "true")
          .schema(getSchema(schemaName))
          .csv(directory_name + filename)
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

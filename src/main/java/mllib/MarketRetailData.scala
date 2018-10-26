package mllib

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.mllib.fpm.FPGrowth
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

object MarketRetailData {

    val def_filename = "src/main/resources/onlineRetailData.csv"
    var dataSetSchema = StructType(Array(
        StructField("InvoiceNo", StringType, nullable = true),
        StructField("StockCode", StringType, nullable = true),
        StructField("Description", StringType, nullable = true),
        StructField("Quantity", IntegerType, nullable = true),
        StructField("InvoiceDate", StringType, nullable = true),
        StructField("UnitPrice", DoubleType, nullable = true),
        StructField("CustomerID", IntegerType, nullable = true),
        StructField("Country", StringType, nullable = true)))

    def main(args: Array[String]): Unit = {

        Logger.getLogger("org").setLevel(Level.OFF)
        Logger.getLogger("akka").setLevel(Level.OFF)

        val csvFile = if (args.isEmpty) def_filename else args(0)

        val sc = new SparkContext("local[2]", "mlLib")

        val sparkSession = SparkSession
          .builder()
          .appName("spark-retail-data")
          .master("local[*]")
          .getOrCreate()

        val retail = sparkSession.read
          .option("header", "true")
          .option("delimiter", ";")
          .option("nullValue", "")
          .option("treatEmptyValuesAsNulls", "true")
          .option("inferSchema", "true")
          .schema(dataSetSchema)
          .csv(csvFile)
        retail.show(10)

        // read csv as a common file into RDD
        val csvFileAsRDD = sc.textFile(csvFile)
        val productsTransactions:
            RDD[Array[String]] = csvFileAsRDD.map(_.split(";"))
        productsTransactions.foreach(x => println(x.mkString("[", " ", "}")))

        // get transactions for goods
        val invoicesWithCodes = productsTransactions.map(x => (x(0), x(1)))
        val invoicesAndStockCodes = invoicesWithCodes.groupByKey()
        val stockCodesAndNames = productsTransactions
          .map(x => (x(1), x(2).toString))
          .collect()
          .toMap

        val goodsWithoutInvoices = invoicesWithCodes.map(x => x._2)
        val rddGoodsWithoutInvoices:
            RDD[Array[String]] = invoicesAndStockCodes.map(_._2.toArray.distinct)

        val fpGrowthTrainingModel = new FPGrowth()
          .setMinSupport(0.02)
          .setNumPartitions(1)
          .run(rddGoodsWithoutInvoices)

        fpGrowthTrainingModel
          .freqItemsets
          .collect()
          .foreach { itemset =>
              println(itemset.items.mkString("[", ",", "]") + ", " + itemset.freq)
          }

        val associationRules = fpGrowthTrainingModel
          .generateAssociationRules(0.01)
          .sortBy(_.confidence, ascending = false)

        associationRules
          .collect()
          .foreach { rule =>
              println(
                  rule.antecedent.map(s => stockCodesAndNames(s)).mkString("[", ",", "]") +
                    " => " +
                    rule.consequent.map(s => stockCodesAndNames(s)).mkString("[", ",", "]") +
                    ", " +
                    rule.confidence)
          }
    }
}

package mllib

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types._

class DataSetSQL(sparkSession: SparkSession, input: String) {
    var dataSetSchema = StructType(Array(
        StructField("InvoiceNo", StringType, nullable = true),
        StructField("StockCode", StringType, nullable = true),
        StructField("Description", StringType, nullable = true),
        StructField("Quantity", IntegerType, nullable = true),
        StructField("InvoiceDate", StringType, nullable = true),
        StructField("UnitPrice", DoubleType, nullable = true),
        StructField("CustomerID", IntegerType, nullable = true),
        StructField("Country", StringType, nullable = true)))

    //Read CSV file to DF and define scheme on the fly
    private val gdelt = sparkSession.read
      .option("header", "true")
      .option("delimiter", ";")
      .option("nullValue", "")
      .option("treatEmptyValuesAsNulls", "true")
      .schema(dataSetSchema)
      .csv(input)

    gdelt.createOrReplaceTempView("dataSetTable")

    //Find the most mentioned actors (persons)
    def getInvoiceNoAndStockCode(): DataFrame = {
        sparkSession.sql(
            " SELECT InvoiceNo, StockCode" +
              " FROM dataSetTable" +
              " WHERE InvoiceNo IS NOT NULL AND StockCode IS NOT NULL")
    }

    def getStockCodeAndDescription(): DataFrame = {
        sparkSession.sql(
            "SELECT DISTINCT StockCode, Description" +
              " FROM dataSetTable" +
              " WHERE StockCode IS NOT NULL AND Description IS NOT NULL")
    }
}
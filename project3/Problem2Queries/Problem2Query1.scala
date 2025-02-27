import org.apache.spark.sql.{SparkSession, DataFrame}

object Query1 {
  def main(args: Array[String]): Unit = {
    //Step 1: Initialize Spark Session
    val spark = SparkSession.builder()
      .appName("Query1")
      .master("local[*]")
      .getOrCreate()

    //Step 2: Load Purchases data from CSV into a DataFrame
    val purchases: DataFrame = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("file:///C:/Users/19788/Desktop/cs4433_projects/project3/Problem2Data/Purchases.csv")

    //Register Purchases as a temporary SQL table
    purchases.createOrReplaceTempView("purchases")

    //Step 3: Filter out purchases where TransTotal > 100
    val filteredPurchases: DataFrame = spark.sql(
      """
        |SELECT * FROM purchases
        |WHERE TransTotal <= 100
        |""".stripMargin
    )

    //Step 4: Save the filtered data as T1
    val outputPath = "file:///C:/Users/19788/Desktop/cs4433_projects/project3/Problem2Query1_output"
    filteredPurchases.write
      .option("header", "true")
      .csv(outputPath)

    println("Query 1 complete! Filtered purchases saved as T1.")

    spark.stop()
  }
}

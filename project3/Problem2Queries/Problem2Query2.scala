import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._

object Query2 {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Query2")
      .master("local[*]")
      .getOrCreate()

    //Step 1: Load the filtered purchases (T1)
    val t1Path = "file:///C:/Users/19788/Desktop/cs4433_projects/project3/Problem2Query1_output"

    val purchasesT1: DataFrame = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(t1Path)

    //Step 2: Group by TransNumItems and calculate min, max, median
    val groupedStats = purchasesT1
      .groupBy("TransNumItems")
      .agg(
        min("TransTotal").alias("MinTotalSpent"),
        max("TransTotal").alias("MaxTotalSpent"),
        expr("percentile_approx(TransTotal, 0.5)").alias("MedianTotalSpent")
      )
      .orderBy("TransNumItems")

    //Step 3: Show results in console
    groupedStats.show()

    //Step 4 (Optional): Save results to a file
    val outputPath = "file:///C:/Users/19788/Desktop/cs4433_projects/project3/Problem2Query2_output"
    groupedStats.write.option("header", "true").csv(outputPath)

    println("Query 2 complete! Results saved to client output.")

    spark.stop()
  }
}

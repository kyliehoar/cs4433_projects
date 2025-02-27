import org.apache.spark.sql.{SparkSession, DataFrame}

object Query3_2 {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Query3_2")
      .master("local[*]")
      .getOrCreate()

    val customers: DataFrame = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("Customers.csv")

    customers.createOrReplaceTempView("customers")

    val purchases: DataFrame = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("Purchases.csv")

    purchases.createOrReplaceTempView("purchases")

    val resultQuery: DataFrame = spark.sql(
      """
        |WITH inexpensive AS (
        | SELECT p.CustID, p.TransTotal, p.TransNumItems
        | FROM purchases p
        | WHERE p.TransTotal <= 100.00
        |)
        |SELECT c.CustID, c.Age, SUM(i.TransNumItems) AS NumItems, ROUND(SUM(i.TransTotal), 2) AS TotalSpent
        |FROM customers c
        |JOIN inexpensive i ON c.CustID = i.CustID
        |WHERE c.Age >= 18 AND c.Age <= 21
        |GROUP BY c.CustID, c.Age
        |""".stripMargin
    )

    resultQuery.write
      .option("header", "true")
      .csv("Query3_2_output")

    spark.stop()
  }
}

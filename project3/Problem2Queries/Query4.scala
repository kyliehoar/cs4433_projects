import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._

object Query4 {
  def main(args: Array[String]): Unit = {
    // Initialize Spark
    val ss = SparkSession.builder()
      .appName("Query4")
      .master("local[*]") // Use all cores on local machine
      .getOrCreate()

    import ss.implicits._

    // Load transactions dataset (T1)
    val transactions: DataFrame = ss.read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("/Users/eloisasalcedo-marx/IdeaProjects/ScalaProject/src/data/T1.csv")
      .withColumnRenamed("TransTotal", "TotalTransaction") // Rename to avoid conflict

    // Load customers dataset
    val customers: DataFrame = ss.read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("/Users/eloisasalcedo-marx/IdeaProjects/ScalaProject/src/data/Customers.csv")


    val transactionsFixed = transactions.withColumnRenamed("CustID", "CustomerID")
    val customersFixed = customers.withColumnRenamed("CustID", "CustomerID")

    // Join transactions with customers
    val joinedData = transactionsFixed.join(customersFixed, "CustomerID")


    // Group transactions by CustomerID, sum total transaction cost, get salary & address
    val totalSpentPerCustomer = joinedData
      .groupBy("CustomerID", "Salary", "Address")
      .agg(sum(col("TotalTransaction").cast("double")).alias("TotalSpent"))
      .withColumn("Salary", col("Salary").cast("double")) // Convert Salary to Double

    val finalOutput = totalSpentPerCustomer
      .select("CustomerID", "TotalSpent", "Salary", "Address") // Select required columns


    finalOutput.show(20, false)

    finalOutput.write
      .format("csv")
      .option("header", "true")
      .mode("overwrite")
      .save("/Users/eloisasalcedo-marx/IdeaProjects/ScalaProject/src/data/final_output.csv")

    println("Full output saved to: /Users/eloisasalcedo-marx/IdeaProjects/ScalaProject/src/data/final_output.csv")

    // Step 3: Filter customers who cannot cover expenses (Salary < TotalSpent)
    val customersWithDeficit = totalSpentPerCustomer
      .filter(col("Salary") < col("TotalSpent"))

    println("Customers who can not afford their expenses: ")
    // Show results
    customersWithDeficit.show(5)

    customersWithDeficit.write
      .format("csv")
      .option("header", "true")
      .mode("overwrite")
      .save("/Users/eloisasalcedo-marx/IdeaProjects/ScalaProject/src/data/CustomersWithDeficit.csv")

    // Stop Spark session
    ss.stop()
  }
}

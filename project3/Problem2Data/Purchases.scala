import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

// Initialize SparkSession
val spark = SparkSession.builder()
  .appName("CombinePurchases")
  .master("local[*]")
  .getOrCreate()

// Initialize the SparkContext from the SparkSession
val sc = spark.sparkContext

// Load Purchases1.csv and Purchases2.csv as RDDs
val purchases1RDD = sc.textFile("file:///C:/Users/19788/Desktop/cs4433_projects/project3/Problem2Data/Purchases1.csv")
val purchases2RDD = sc.textFile("file:///C:/Users/19788/Desktop/cs4433_projects/project3/Problem2Data/Purchases2.csv")

// Combine both datasets by performing a union
val combinedPurchasesRDD = purchases1RDD.union(purchases2RDD)

// Save the combined dataset as Purchases.csv
val localOutputPath = "file:///C:/Users/19788/Desktop/cs4433_projects/project3/Problem2Data/Purchases.csv"
combinedPurchasesRDD.saveAsTextFile(localOutputPath)

println("Combining complete! Check the directory for Purchases.csv.")

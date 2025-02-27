import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

//Initialize SparkSession (replaces SparkContext directly)
val spark = SparkSession.builder()
  .appName("Query2")
  .master("local[*]")
  .getOrCreate()

//Initialize the SparkContext from the SparkSession
val sc = spark.sparkContext

//Load Mega-Event-No-Disclosure file
val megaEventRDD = sc.textFile("file:///C:/Users/19788/Desktop/cs4433_projects/project3/Problem1Data/Meta-Event-No-Disclosure.csv")
//Load Reported-Illnesses file
val illnessRDD = sc.textFile("file:///C:/Users/19788/Desktop/cs4433_projects/project3/Problem1Data/Reported-Illnesses.csv")

//Parse Mega-Event-No-Disclosure data (skip the header)
val megaEventHeader = megaEventRDD.first()
val megaEventDataRDD = megaEventRDD
  .filter(row => row != megaEventHeader)
  .map(line => {
    val cols = line.split(",")
    if (cols.length >= 4) {
      Some((cols(0), cols(1), cols(2), cols(3)))
    } else {
      None
    }
  }).flatMap(x => x)

//Parse Reported-Illnesses data (skip the header)
val illnessHeader = illnessRDD.first()
val illnessDataRDD = illnessRDD
  .filter(row => row != illnessHeader)
  .map(line => {
    val cols = line.split(",")
    if (cols.length >= 2) {
      Some((cols(0), cols(1)))
    } else {
      None
    }
  }).flatMap(x => x)

//Filter reported sick attendees from Reported-Illnesses file
val sickAttendeesRDD = illnessDataRDD.filter(_._2 == "sick")

//Join Mega-Event data with sick attendees on id
val sickAttendeesWithDetails = megaEventDataRDD
  .keyBy(_._1)
  .join(sickAttendeesRDD.keyBy(_._1))
  .map { case (_, ((name, table, test), _)) => (name, table, test) }

//Display the result
sickAttendeesWithDetails.collect().foreach(println)

//Save results locally
val localOutputPath = "file:///C:/Users/19788/Desktop/cs4433_projects/project3/query2_output"
sickAttendeesWithDetails.saveAsTextFile(localOutputPath)


//Load Meta-Event.csv from local file system
val eventRDD = sc.textFile("C:/Users/19788/Desktop/cs4433_projects/project3/Problem1Data/Meta-Event.csv")

//Extract header and remove it
val header = eventRDD.first()
val dataRDD = eventRDD.filter(row => row != header)

//Parse the CSV (id, name, table, test)
val parsedRDD = dataRDD.map(line => {
  val cols = line.split(",")
  (cols(0), cols(1), cols(2), cols(3))
})

//Filter sick attendees
val sickAttendeesRDD = parsedRDD.filter(_._4 == "sick")

//Save results locally
val localOutputPath = "file:///C:/Users/19788/Desktop/cs4433_projects/project3/query1_output"
sickAttendeesRDD.map(x => x.toString()).saveAsTextFile(localOutputPath)


println(s"Results saved locally at: $localOutputPath")


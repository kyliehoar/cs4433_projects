import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import java.io.{File, PrintWriter}

// ***** PROBLEM 1, QUERY 3 *****
// Assume you are given the file Mega-Event, return all healthy people pi in the Mega-Event file that
// were sitting on the same table with at least one sick person pj, so that they can be notified to take
// appropriate precaution. Do not return a healthy person twice.

object Query3_1 {

  def main(args: Array[String]): Unit = {

    val sparConf = new SparkConf().setMaster("local").setAppName("Query3_1")

    val sc = new SparkContext(sparConf)

    val lines: RDD[String] = sc.textFile("Meta-Event.csv")

    val header = lines.first()
    val data = lines.filter(_ != header)

    val people: RDD[(String, String, String, String)] = data.map { line =>
      val cols = line.split(",")
      (cols(0).trim, cols(1).trim, cols(2).trim, (cols(3).trim))
    }

    val sickTablesSet = sc.broadcast( people
      .filter { case (_, _, _, test) => test == "sick" }
      .map { case (_, _, table, _) => table }
      .distinct()
      .collect()
      .toSet
    )

    val healthyPeople: RDD[(String, String)] = people
      .filter { case (_, _, table, test) => test == "not-sick" && sickTablesSet.value.contains(table) }
      .map { case (id, name, _, _) => (id, name) }
      .distinct()

    val resultArray: Array[(String, String)] = healthyPeople.collect()

    val outputFile = new File("Query3_1_output.txt")
    val writer = new PrintWriter(outputFile)

    println("Healthy people who need to be notified:")
    resultArray.foreach { case (id, name) => writer.println(s"ID: $id, Name: $name") }

    sc.stop()
  }
}

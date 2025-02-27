import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import java.io.{File, PrintWriter}

// ***** PROBLEM 1, QUERY 4 *****
// Assume you are given the file Mega-Event. Return for each table table-i, the number of people pi
// that were sitting on this same table and a Boolean flag indicating if the table housed all healthy
// people as flag=healthy, otherwise return flag=concern. If no one was sitting on a table, do not
// return that table.

object Query4_1 {

  def main(args: Array[String]): Unit = {

    val sparConf = new SparkConf().setMaster("local").setAppName("Query4_1")

    val sc = new SparkContext(sparConf)

    val lines: RDD[String] = sc.textFile("Meta-Event.csv")

    val header = lines.first()
    val data = lines.filter(_ != header)

    val people: RDD[(String, String, String, String)] = data.map { line =>
      val cols = line.split(",")
      (cols(0).trim, cols(1).trim, cols(2).trim, cols(3).trim)
    }

    val tableGroup: RDD[(String, Iterable[(String, String, String, String)])] = people
      .groupBy( { case (_, _, table, _) => table })

    val tableTests: RDD[(String, String, Int)] = tableGroup
      .map { case (table, people) =>
      val isSickTable = people.exists { case (_, _, _, test) => test == "sick"}
      val label = if (isSickTable) "concern" else "healthy"
        (table, label, people.size) }

    val removeEmpties: RDD[(String, String, Int)] = tableTests
      .filter { case (_, _, count) =>
        count != 0
      }

    val outputFile = new File("Query4_1_output.txt")
    val writer = new PrintWriter(outputFile)

    removeEmpties.collect().foreach { case (table, label, count) =>
      writer.println(s"Table: $table, Status: $label, Number of People: $count")
    }
//
//    removeEmpties.collect().foreach { case (table, label, count) =>
//      println(s"Table: $table, Status: $label, Number of People: $count")
//    }

    sc.stop()
  }
}

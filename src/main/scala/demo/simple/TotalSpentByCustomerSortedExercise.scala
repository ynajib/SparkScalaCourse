package demo.simple

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._

/**
  * Created by sromero on 3/2/17.
  */
object TotalSpentByCustomerSortedExercise {

  /** Our main function where the action happens */
  def main(args: Array[String]) {

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkContext using every core of the local machine
    val sc = new SparkContext("local[*]", "TotalSpentByCustomerSortedExercise")

    val lines = sc.textFile("/Users/sromero/Development/SparkScala/customer-orders.csv")

    val customers = lines.map(line => {

      val fields = line.split(",")

      val customerId = fields(0).toInt
      val price = fields(2).toFloat

      (customerId, price)
    })

    val totalPriceByCustomer = customers.reduceByKey( (x,y) => x + y)

    val flipped = totalPriceByCustomer.map( x => (x._2, x._1) )

    val totalByCustomerSorted = flipped.sortByKey()

    val results = totalByCustomerSorted.collect()

    results.foreach(println)

  }

}

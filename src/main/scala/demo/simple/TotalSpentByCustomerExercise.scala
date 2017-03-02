package demo.simple

import org.apache.spark._
import org.apache.log4j._

/**
  * Created by sromero on 3/2/17.
  */
object TotalSpentByCustomerExercise {

  /** Our main function where the action happens */
  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[*]", "TotalSpentByCustomerExercise")

    val lines = sc.textFile("/Users/sromero/Development/SparkScala/customer-orders.csv")

    val customers = lines.map(line => {

      val fields = line.split(",")

      val customerId = fields(0).toInt
      val price = fields(2).toFloat

      (customerId, price)
    })

    val totalPriceByCustomer = customers.reduceByKey( (x,y) => x + y)

    val results = totalPriceByCustomer.collect()

    results.sorted.foreach(println)

  }

}

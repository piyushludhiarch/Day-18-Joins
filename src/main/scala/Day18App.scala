import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Day18App {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Day 18 - Joins")
      .master("local[*]")
      .config("spark.ui.enabled", "false")
      .getOrCreate()

    import spark.implicits._

    println("===== DAY 18: JOINS =====")

    val customers = Seq(
      (1, "Amit", "Delhi"),
      (2, "Rahul", "Mumbai"),
      (3, "Sara", "Bangalore"),
      (4, "Neha", "Pune")
    ).toDF(
      "customer_id",
      "customer_name",
      "city"
    )

    val orders = Seq(
      (101, 1, 5000.0),
      (102, 1, 3000.0),
      (103, 2, 7000.0),
      (104, 5, 9000.0)
    ).toDF(
      "order_id",
      "customer_id",
      "amount"
    )

    val payments = Seq(
      (101, "Paid"),
      (102, "Pending"),
      (103, "Paid")
    ).toDF(
      "order_id",
      "payment_status"
    )

    println("\nINNER JOIN:")

    customers
      .join(orders, Seq("customer_id"), "inner")
      .show(false)

    println("\nLEFT JOIN:")

    customers
      .join(orders, Seq("customer_id"), "left")
      .show(false)

    println("\nRIGHT JOIN:")

    customers
      .join(orders, Seq("customer_id"), "right")
      .show(false)

    println("\nFULL JOIN:")

    customers
      .join(orders, Seq("customer_id"), "full")
      .show(false)

    println("\nALIASES AND AMBIGUOUS COLUMNS:")

    val c = customers.alias("c")
    val o = orders.alias("o")

    c.join(
      o,
      col("c.customer_id") === col("o.customer_id"),
      "inner"
    )
      .select(
        col("c.customer_id"),
        col("c.customer_name"),
        col("o.order_id"),
        col("o.amount")
      )
      .show(false)

    println("\nLEFT JOIN WITH NULL HANDLING:")

    customers
      .join(orders, Seq("customer_id"), "left")
      .withColumn(
        "order_amount",
        coalesce(col("amount"), lit(0.0))
      )
      .show(false)

    println("\nORDERS + CUSTOMERS + PAYMENTS:")

    orders
      .join(customers, Seq("customer_id"), "left")
      .join(payments, Seq("order_id"), "left")
      .show(false)

    println("\nSHUFFLE SORT MERGE JOIN:")
    println("Spark can use Shuffle Sort Merge Join for large datasets.")
    println("Both datasets may be shuffled by the join key and sorted before joining.")

    spark.stop()
  }
}

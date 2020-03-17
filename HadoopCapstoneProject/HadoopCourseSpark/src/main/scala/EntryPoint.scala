import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrameWriter, Dataset, SaveMode, SparkSession}
import org.apache.spark.{HashPartitioner, Partitioner}

case class CategoryAndProductsAmount(product_category: String, products_sold: Long)
case class ProductAmountPerCategory(product_category: String, product_name: String, products_sold: Long)
case class CountryAndRevenue(country_name: String, money_spent: Double)

case class Purchase(product_name: String,
                    product_category: String,
                    product_price: Double,
                    tstmp: String,
                    client_ip: String)

case class IdCountryName(geoname_id: String,
                         country_name: String)

case class IdNetwork(geoname_id: String,
                     network: String)

object EntryPoint {
  val hdfsServer = "hdfs://sandbox-hdp.hortonworks.com:8020"
  val eventsRoot = hdfsServer + "/user/root/flume"
  val jdbcCS = "jdbc:mysql://sandbox-hdp.hortonworks.com/big_data"
  val hdfsGeoIps = hdfsServer + "/user/root/import/geo_ips.csv"
  val hdfsCountryNetworks = hdfsServer + "/user/root/import/geo_locations.csv"
  val hdfsCheckpointDirectory = hdfsServer + "/user/root/spark_checkpoint"

  def main(args: Array[String]): Unit = {
    implicit val spark: SparkSession = SparkSession.builder.appName("Simple Application").getOrCreate()
    implicit val sparkIoRDD: SparkIO[RDD] = SparkRddIO
    implicit val sparkIoDataset: SparkIO[Dataset] = SparkDatasetIO

    println("RDD")
    println("I    most purchased categories")
    mostPurchasedCategories("spark_rdd_top_purchased_categories", SparkRDDRequests)
    println("II   most products per category")
    mostPurchasedProductsPerCategory("spark_rdd_top_purchased_products_per_category", SparkRDDRequests)
    println("III  most spending countries")
    mostSpendingCountries("spark_rdd_top_spending_countries", SparkRDDRequests)

    println("SQL")
    println("I    most purchased categories")
    mostPurchasedCategories("spark_sql_top_purchased_categories", SparkSQLRequests)
    println("II   most products per category")
    mostPurchasedProductsPerCategory("spark_sql_top_purchased_products_per_category", SparkSQLRequests)
    println("most spending countries")
//    // This last request I was not able to optimize in Spark SQL. Still the code is presented.
//    println("III  most spending countries")
//    mostSpendingCountries("spark_sql_top_spending_countries", SparkSQLRequests)
    println("FINISH")
    spark.stop()
  }

  def setOptions[T](writer: DataFrameWriter[T], tableName: String): DataFrameWriter[T] = {
    writer
      .format("jdbc")
      .option("url", jdbcCS)
      .option("dbtable", tableName)
      .option("user", "root")
      .option("password", "hortonworks1")
  }

  def mostPurchasedCategories[M[_]](tableName: String, requests: SparkRequests[M])(implicit spark: SparkSession, sparkIO: SparkIO[M]): Unit = {
    import spark.implicits._
    val events = sparkIO.loadEvents(eventsRoot)
    val r = requests.mostPurchasedCategories(events).toDS()
    setOptions(r.write, tableName).mode(SaveMode.Overwrite).save()
  }

  def mostPurchasedProductsPerCategory[M[_]](tableName: String, requests: SparkRequests[M])(implicit spark: SparkSession, sparkIO: SparkIO[M]): Unit = {
    import spark.implicits._
    val events = sparkIO.loadEvents(eventsRoot)
    val r = requests.mostPurchasedProductsPerCategory(events)
    setOptions(sparkIO.getWriter(r), tableName).mode(SaveMode.Overwrite).save()
  }

  def mostSpendingCountries[M[_]](tableName: String, requests: SparkRequests[M])(implicit spark: SparkSession, sparkIO: SparkIO[M]): Unit = {
    import spark.implicits._
    val geoIps = sparkIO.loadCityGeo(hdfsGeoIps)
    val countryNetworks = sparkIO.loadCountryNetworks(hdfsCountryNetworks)
    val events = sparkIO.loadEvents(eventsRoot)
    val result = requests.mostSpendingCountries(geoIps, countryNetworks, events).toDS()
    setOptions(result.write, tableName).mode(SaveMode.Overwrite).save()
  }
}

object IpFormatter {
  def maskIp(ip: Int, networkSize: Int): Int =
    if (networkSize == 0) 0 else {
      val emptyBits = 32 - networkSize
      -1 >>> emptyBits << emptyBits & ip
    }


  def ipToMaskAndNetwork(ip: String): (Int, Int) = {
    val Array(network, mask) = ip.split('/')
    (ipToInt(network), mask.toInt)
  }

  def ipToInt(ip: String): Int = ip.split('.').foldLeft(0) { (acc, cur) =>
    val part = cur.toInt
    require(part < 0 || part > 255, "ip {" + ip + "} is malformed")
    (acc << 8) + part
  }

  def maskIp(ip: String, networkSize: Int): Int = {
    val emptyBits = 32 - networkSize
    -1 >>> emptyBits << emptyBits & ipToInt(ip)
  }
}

class MyPartitioner(partitions: Int, hash: Any => Int) extends Partitioner {
  require(partitions >= 0, s"Number of partitions ($partitions) cannot be negative.")

  def nonNegativeMod(x: Int, mod: Int): Int = {
    val rawMod = x % mod
    rawMod + (if (rawMod < 0) mod else 0)
  }

  def numPartitions: Int = partitions

  def getPartition(key: Any): Int = key match {
    case null => 0
    case _ => nonNegativeMod(hash(key), numPartitions)
  }

  override def equals(other: Any): Boolean = other match {
    case h: HashPartitioner =>
      h.numPartitions == numPartitions
    case _ =>
      false
  }

  override def hashCode: Int = numPartitions
}
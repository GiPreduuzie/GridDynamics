import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{count, row_number, sum}
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.storage.StorageLevel

object SparkSQLRequests extends SparkRequests[Dataset] {
  def mostPurchasedCategories(events: Dataset[Purchase])(implicit spark: SparkSession): Seq[CategoryAndProductsAmount] = {
    import spark.implicits._
    events.groupByKey(_.product_category).count().orderBy($"count(1)".desc).take(10).map(x => CategoryAndProductsAmount(x._1, x._2))
  }

  override def mostPurchasedProductsPerCategory(events: Dataset[Purchase])(implicit spark: SparkSession): Dataset[ProductAmountPerCategory] = {
    import spark.implicits._
    events.groupBy($"product_category", $"product_name")
      .agg(count("product_name").as("products_sold"))
      .withColumn("row_number", row_number.over(Window.partitionBy("product_category").orderBy($"products_sold".desc, $"product_name".asc)))
      .where($"row_number" <= 10)
      .orderBy($"product_category".asc, $"row_number".asc)
      .drop("row_number")
      .as[ProductAmountPerCategory]
  }

  override def mostSpendingCountries(geoIps: Dataset[IdNetwork], countryNetworks: Dataset[IdCountryName], events: Dataset[Purchase])(implicit spark: SparkSession): Seq[CountryAndRevenue] = {
    import org.apache.spark.sql.functions.udf
    import spark.implicits._

    val getNetworkSize = udf { x: String => x.split('/')(1) }
    val ipToInt = udf { x: String => IpFormatter.ipToInt(x.split('/')(0)) }
    val maskInt = udf { (x: Int, bits: Int) => IpFormatter.maskIp(x, bits) }

    val geoData = geoIps.select("network", "geoname_id").withColumnRenamed("geoname_id", "geo_ips_geoname_id")
      .join(countryNetworks.select("geoname_id", "country_name")).where($"geo_ips_geoname_id" === $"geoname_id")
      .withColumn("network_size", getNetworkSize($"network"))
      .withColumn("network_ip", ipToInt($"network"))
      .persist(StorageLevel.MEMORY_AND_DISK)

    val networkSizes = geoData
      .select("network_size")
      .withColumnRenamed("network_size", "mask_size")
      .distinct()
      .persist(StorageLevel.MEMORY_AND_DISK)

    val maskedIpEvents = events
      .crossJoin(networkSizes)
      .withColumn("masked_ip", maskInt(ipToInt($"client_ip"), $"mask_size"))
      .persist(StorageLevel.MEMORY_AND_DISK)

    val eventsWithCountries =
      geoData
        .join(maskedIpEvents)
        .where($"mask_size" === $"network_size" and $"network_ip" === $"masked_ip")

    val result = eventsWithCountries
      .groupBy("country_name").agg(sum("product_price")
      .as("money_spent")).orderBy($"money_spent".desc).as[CountryAndRevenue].take(10)

    geoData.unpersist()
    networkSizes.unpersist()
    maskedIpEvents.unpersist()
    eventsWithCountries.unpersist()

    result
  }
}
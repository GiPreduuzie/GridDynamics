import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

object SparkRDDRequests extends SparkRequests[RDD] {

  def mostPurchasedCategories(events: RDD[Purchase])(implicit spark: SparkSession): Seq[CategoryAndProductsAmount] = {
    val ordering = new Ordering[CategoryAndProductsAmount] {
      override def compare(x: CategoryAndProductsAmount, y: CategoryAndProductsAmount): Int = y.products_sold.compare(x.products_sold)
    }

    events
      .keyBy(_.product_category)
      .aggregateByKey(0)((count, _) => count + 1, _ + _)
      .map(x => CategoryAndProductsAmount(x._1, x._2))
      .takeOrdered(10)(ordering)
  }

  def mostPurchasedProductsPerCategory(events: RDD[Purchase])(implicit spark: SparkSession): RDD[ProductAmountPerCategory] = {
    events
      .keyBy(x => (x.product_category, x.product_name)).mapValues(_ => 1)
      .repartitionAndSortWithinPartitions(new MyPartitioner(4, _.asInstanceOf[(String, String)]._1.hashCode))
      .mapPartitions(it => new GroupingIterator(it))
      .keyBy(x => (x._1._1, -x._2))
      .mapValues(x => x._1._2)
      .repartitionAndSortWithinPartitions(new MyPartitioner(4, _.asInstanceOf[(String, Int)]._1.hashCode))
      .mapPartitions(_.take(10))
      .map(kvs => ProductAmountPerCategory(kvs._1._1, kvs._2, -kvs._1._2))
  }

  def mostSpendingCountries(geoIps: RDD[IdNetwork], countryNetworks: RDD[IdCountryName], events: RDD[Purchase])(implicit spark: SparkSession): Seq[CountryAndRevenue] = {
    val countiesNetworks = geoIps.keyBy(_.geoname_id)
      .join(countryNetworks.keyBy(_.geoname_id))
      .map { case (_, (idNetwork, idCountryName)) => IpFormatter.ipToMaskAndNetwork(idNetwork.network) -> idCountryName.country_name }
      .persist(StorageLevel.MEMORY_AND_DISK)

    val ipWithCountries = events.flatMap { x =>
      val ip = IpFormatter.ipToInt(x.client_ip)
      (1 to 32).map(networkSize => IpFormatter.maskIp(ip, networkSize) -> networkSize).map(_ -> x)
    }.join(countiesNetworks)

    val ordering = new Ordering[CountryAndRevenue] {
      override def compare(x: CountryAndRevenue, y: CountryAndRevenue): Int = y.money_spent.compare(x.money_spent)
    }

    ipWithCountries
      .keyBy(x => x._2._2)
      .mapValues(_._2._1.product_price)
      .aggregateByKey(0.0)(_ + _, _ + _)
      .map(x => CountryAndRevenue(x._1, x._2))
      .takeOrdered(10)(ordering)
  }
}

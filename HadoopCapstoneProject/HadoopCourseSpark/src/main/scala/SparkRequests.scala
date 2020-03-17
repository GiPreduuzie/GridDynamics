import org.apache.spark.sql.SparkSession

trait SparkRequests[M[_]] {
  def mostPurchasedCategories(events: M[Purchase])(implicit spark: SparkSession): Seq[CategoryAndProductsAmount]
  def mostPurchasedProductsPerCategory(events: M[Purchase])(implicit spark: SparkSession): M[ProductAmountPerCategory]
  def mostSpendingCountries(geoIps: M[IdNetwork], countryNetworks: M[IdCountryName], events: M[Purchase])(implicit spark: SparkSession): Seq[CountryAndRevenue]
}
import org.apache.spark.sql.SparkSession
import org.scalatest._
import org.scalatest.matchers.should.Matchers


class SparkRequestsSpec extends WordSpec with Matchers with SparkContextSetup {
  "Spark SQL" should {
    "calculate most purchased categories" in withSparkContext { implicit spark =>
      import spark.implicits._

      val data = Seq(
        ("gadgets", "smartphone"),
        ("cloth", "boots"),
        ("cloth", "shirt"),
        ("cloth", "pans"),
        ("tools", "drill"),
        ("tools", "hummer")
      ).map(x => Purchase(x._2, x._1, 10, "", "127.0.0.1"))

      val rdd = spark.sparkContext.parallelize(data)
      val resultRDD = SparkRDDRequests.mostPurchasedCategories(rdd)
      val resultSQL = SparkSQLRequests.mostPurchasedCategories(rdd.toDS())
      val expected = Seq(
        CategoryAndProductsAmount("cloth", 3),
        CategoryAndProductsAmount("tools", 2),
        CategoryAndProductsAmount("gadgets", 1))

      resultRDD shouldBe expected
      resultSQL shouldBe expected
    }

    "calculate most purchased products per category" in withSparkContext { implicit spark =>
      import spark.implicits._

      val data = Seq(
        ("gadgets", "laptop"),
        ("gadgets", "smartphone"),
        ("gadgets", "smartphone"),
        ("cloth", "boots"),
        ("cloth", "boots"),
        ("cloth", "boots"),
        ("cloth", "shirt"),
        ("cloth", "shirt"),
        ("cloth", "pans"),
        ("tools", "drill"),
        ("tools", "hummer"),
        ("tools", "hummer")
      ).map(x => Purchase(x._2, x._1, 10, "", "127.0.0.1"))

      val rdd = spark.sparkContext.parallelize(data)
      val resultRDD = SparkRDDRequests.mostPurchasedProductsPerCategory(rdd).take(100).toSeq
      val resultSQL = SparkSQLRequests.mostPurchasedProductsPerCategory(rdd.toDS()).take(100).toSeq
      val expected = Seq(
        ProductAmountPerCategory("cloth", "boots", 3),
        ProductAmountPerCategory("cloth", "shirt", 2),
        ProductAmountPerCategory("cloth", "pans", 1),
        ProductAmountPerCategory("gadgets", "smartphone", 2),
        ProductAmountPerCategory("gadgets", "laptop", 1),
        ProductAmountPerCategory("tools", "hummer", 2),
        ProductAmountPerCategory("tools", "drill", 1))

      resultRDD shouldBe expected
      resultSQL shouldBe expected
    }

    "calculate most spending countries" in withSparkContext { implicit spark =>
      import spark.implicits._

      val geoIps = spark.sparkContext.parallelize(Seq(
        IdNetwork("1", "127.231.0.0/16"),
        IdNetwork("2", "128.0.0.0/8"),
        IdNetwork("3", "127.111.1.120/32"),
        IdNetwork("4", "127.233.3.0/24"),
        IdNetwork("5", "18.37.0.0/16")))

      val countryNetworks = spark.sparkContext.parallelize(Seq(
        IdCountryName("1", "Russia"),
        IdCountryName("2", "Georgia"),
        IdCountryName("3", "Ukraine"),
        IdCountryName("4", "Belorus"),
        IdCountryName("5", "Armenia")))

      val events = spark.sparkContext.parallelize(Seq(
        ("gadgets", "laptop", "127.231.1.2", 300),     // 1
        ("gadgets", "smartphone", "127.231.6.2", 100), // 1
        ("gadgets", "smartphone", "128.255.1.3", 150), // 2
        ("cloth", "boots", "127.111.1.120", 50),       // 3
        ("cloth", "boots", "18.37.19.1", 1000),        // 5
        ("cloth", "boots", "127.233.3.24", 10),        // 4
        ("cloth", "shirt", "18.37.7.0", 5),            // 5
        ("cloth", "shirt", "128.2.1.3", 4),            // 2
        ("cloth", "pans", "127.233.3.32", 35),         // 4
        ("tools", "drill", "127.111.1.120", 370),      // 3
        ("tools", "hummer", "128.0.0.1", 18),          // 2
        ("tools", "hummer", "18.37.7.0", 23)           // 5
      ).map(x => Purchase(x._2, x._1, x._4, "", x._3)))

      val resultRDD = SparkRDDRequests.mostSpendingCountries(geoIps, countryNetworks, events).take(100)
      val resultSQL = SparkSQLRequests.mostSpendingCountries(geoIps.toDS(), countryNetworks.toDS(), events.toDS()).take(100)
      val expected = Seq(
        CountryAndRevenue("Armenia", 1028),
        CountryAndRevenue("Ukraine", 420),
        CountryAndRevenue("Russia", 400),
        CountryAndRevenue("Georgia", 172),
        CountryAndRevenue("Belorus", 45))

      resultRDD shouldBe expected
      resultSQL shouldBe expected
    }
  }
}

trait SparkContextSetup {
  def withSparkContext(testMethod: SparkSession => Any) {
    val spark = SparkSession.builder()
      .master("local")
      .appName("Spark test")
      .getOrCreate()

    try {
      testMethod(spark)
    }
    finally spark.stop()
  }
}
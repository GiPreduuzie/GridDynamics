import java.util.Properties

import org.apache.hadoop.fs.{LocatedFileStatus, RemoteIterator}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, DataFrameWriter, SaveMode, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{HashPartitioner, Partitioner}

import scala.annotation.tailrec

case class CategoryAndProductsAmount(category: String, products_sold: Long)
case class CategoryProductAndRevenue(category: String, product: String, revenue: Double)
case class CountryAndRevenue(country: String, revenue: Double)

case class Purchase(product_name: String,
                    product_category: String,
                    product_price: Double,
                    tstmp: String,
                    client_ip: String)

case class IdCountryName(geoname_id: String,
                         country_name: String)

case class IdNetwork(geoname_id: String,
                     network: String)

object IpFormatter {
  def ipToMaskAndNetwork(ip: String): (Int, Int) = {
    val Array(network, mask) = ip.split('/')
    (ipToInt(network), mask.toInt)
  }
  def ipToInt(ip: String): Int = ip.split('.').foldLeft(0)((acc, cur) => (acc << 8) + cur.toInt)
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

class GroupingIterator[K](internal: Iterator[(K, Int)]) extends Iterator[(K, Int)] with Serializable {
  var firstInGroupOpt: Option[(K, Int)] = None

  override def hasNext: Boolean = firstInGroupOpt.nonEmpty || internal.hasNext

  override def next(): (K, Int) = {
    val firstInGroup = firstInGroupOpt.getOrElse(internal.next())

    @tailrec
    def loop(accum: Int, current: (K, Int)): (Int, (K, Int)) = {
      if (internal.hasNext && current._1 == firstInGroup._1) {
        loop(accum + current._2, internal.next())
      } else (accum, current)
    }

    val (accum, current) = loop(0, firstInGroup)

    val (firstOpt, acc) = if (current._1 == firstInGroup._1) (None, accum + firstInGroup._2) else (Some(current), accum)
    firstInGroupOpt = firstOpt
    (firstInGroup._1, acc)
  }
}

object SparkIO extends Serializable {
  def loadEvents(spark: SparkSession, hdfsEventsRoot: String): DataFrame = {
    import org.apache.hadoop.fs.{FileSystem, Path}

    def rec(it: RemoteIterator[LocatedFileStatus]): List[LocatedFileStatus] =
      if (it.hasNext) it.next() :: rec(it) else Nil

    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    val files = fs.listFiles(new Path(hdfsEventsRoot), true)
    val paths = rec(files).filter(_.isFile).map(_.getPath.toString)

    val schema =
      StructType(List("product_name", "product_category", "product_price", "tstmp", "client_ip")
        .map {
          case "product_price" => StructField("product_price", DoubleType)
          case x => StructField(x, StringType)
        })
    spark.read.schema(schema).csv(paths: _*)
  }

  def loadCityGeo(spark: SparkSession, hdfsGeoIps: String): DataFrame =
    spark.read.option("header", "true").csv(hdfsGeoIps)

  def loadCountryNetworks(spark: SparkSession, hdfsCountryNetworks: String): DataFrame =
    spark.read.option("header", "true").csv(hdfsCountryNetworks)
}

object EntryPoint {
  val hdfsServer = "hdfs://sandbox-hdp.hortonworks.com:8020"
  val eventsRoot = hdfsServer + "/user/root/flume"
  val jdbcCS = "jdbc:mysql://sandbox-hdp.hortonworks.com/big_data"
  val hdfsGeoIps = hdfsServer + "/user/root/import/geo_ips.csv"
  val hdfsCountryNetworks = hdfsServer + "/user/root/import/geo_locations.csv"
  val hdfsCheckpointDirectory = hdfsServer + "/user/root/spark_checkpoint"

  def main(args: Array[String]) = {
    val spark = SparkSession.builder.appName("Simple Application").getOrCreate()

    println("RDD")
    println("I    most purchased categories")
    mostPurchasedCategoriesRDD(spark, "spark_rdd_top_purchased_categories")
    println("II   most products per category")
    mostPurchasedProductsPerCategoryRDD(spark, "spark_rdd_top_purchased_products_per_category")
    println("III  most spending countries")
    mostSpendingCountriesRDD(spark, "spark_rdd_top_spending_countries")

    println("SQL")
    println("I    most purchased categories")
    mostPurchasedCategories(spark, "spark_sql_top_purchased_categories")
    println("II   most products per category")
    mostPurchasedProductsPerCategory(spark, "spark_sql_top_purchased_products_per_category")
    println("most spending countries")
    mostSpendingCountriesBruteForce(spark)
    // This last request I was not able to optimize in Spark SQL. Still the code is presented.
    // println("III  most spending countries")
    // mostSpendingCountries(spark, "spark_sql_top_spending_countries")


    println("FINISH")
    spark.stop()
  }

  def setOptions[T](writer: DataFrameWriter[T], tableName: String) = {
    writer
      .format("jdbc")
      .option("url", jdbcCS)
      .option("dbtable", tableName)
      .option("user", "root")
      .option("password", "hortonworks1")
  }

  // ------ SPARK SQL -----

  def mostPurchasedCategories(spark: SparkSession, tableName: String) = {
    import spark.implicits._
    val df = SparkIO.loadEvents(spark, eventsRoot).as[Purchase]
    val r = df.groupByKey(_.product_category).count().orderBy($"count(1)".desc).take(10)

    setOptions(r.toSeq.map(x => CategoryAndProductsAmount(x._1, x._2)).toDS().write, tableName).mode(SaveMode.Overwrite).save()
  }

  def mostPurchasedProductsPerCategory(spark: SparkSession, tableName: String) = {
    import spark.implicits._

    val events = SparkIO.loadEvents(spark, eventsRoot);
    val r =
      events.groupBy($"product_category", $"product_name")
        .agg(count("product_name").as("count"))
        .withColumn("row_number", row_number.over(Window.partitionBy("product_category").orderBy($"count".desc)))
        .where($"row_number" < 10)
        .orderBy($"product_category".asc, $"row_number".asc)

    setOptions(r.write, tableName).mode(SaveMode.Overwrite).save()
  }

  def mostSpendingCountriesBruteForce(spark: SparkSession) = {
    // /usr/share/java/mysql-connector-java.jar
    import org.apache.spark.sql.functions.udf
    import spark.implicits._

    val getNetworkSize = udf { x: String => x.split('/')(1) }
    val ipToInt = udf { x: String => IpFormatter.ipToInt(x.split('/')(0)) }
    val maskInt = udf { (x: Int, bits: Int) => -1 >>> bits << bits & x }

    val geoIps = SparkIO.loadCityGeo(spark, hdfsGeoIps)
    val countryNetworks = SparkIO.loadCountryNetworks(spark, hdfsCountryNetworks)
    val events = SparkIO.loadEvents(spark, eventsRoot)

    println("loaded. Starting the job")
    spark.sparkContext.setCheckpointDir(hdfsCheckpointDirectory)

    val geoData = geoIps.select("network", "geoname_id").withColumnRenamed("geoname_id", "geo_ips_geoname_id")
      .join(countryNetworks.select("geoname_id", "country_name")).where($"geo_ips_geoname_id" === $"geoname_id")
      .withColumn("network_size", getNetworkSize($"network"))
      .withColumn("network_ip", ipToInt($"network"))

    println("step 1")

    val eventsWithCountries =
      geoData
        .join(events.withColumn("ip_int", ipToInt($"client_ip")))
        .where($"network_ip" === maskInt($"ip_int", $"network_size"))

    println("step 2")

    val q = eventsWithCountries
      .groupBy("country_name").agg(sum("product_price")
      .as("money_spent")).orderBy("money_spent").take(10)


    println("final result*: \n")

    // output
    println(q.take(100).mkString("\n"))
  }

  def mostSpendingCountries(spark: SparkSession, tableName: String) = {
    // /usr/share/java/mysql-connector-java.jar
    import org.apache.spark.sql.functions.udf
    import spark.implicits._

    val getNetworkSize = udf { x: String => x.split('/')(1) }
    val ipToInt = udf { x: String => IpFormatter.ipToInt(x.split('/')(0)) }
    val maskInt = udf { (x: Int, bits: Int) => -1 >>> bits << bits & x }

    val geoIps = SparkIO.loadCityGeo(spark, hdfsGeoIps)
    val countryNetworks = SparkIO.loadCountryNetworks(spark, hdfsCountryNetworks)
    val events = SparkIO.loadEvents(spark, eventsRoot)

    println("loaded. Starting the job")
    spark.sparkContext.setCheckpointDir(hdfsCheckpointDirectory)

    val geoData = geoIps.select("network", "geoname_id").withColumnRenamed("geoname_id", "geo_ips_geoname_id")
      .join(countryNetworks.select("geoname_id", "country_name")).where($"geo_ips_geoname_id" === $"geoname_id")
      .withColumn("network_size", getNetworkSize($"network"))
      .withColumn("network_ip", ipToInt($"network"))
      .checkpoint()
      .persist(StorageLevel.MEMORY_AND_DISK)

    println("step 1")

    val networkSizes = geoData
      .select("network_size")
      .withColumnRenamed("network_size", "mask_size")
      .distinct()
      .checkpoint()
      .persist(StorageLevel.MEMORY_AND_DISK)

    println("step 2")

    val maskedIpEvents = events
      .crossJoin(networkSizes)
      .withColumn("masked_ip", maskInt(ipToInt($"client_ip"), $"mask_size"))
      .checkpoint()
      .persist(StorageLevel.MEMORY_AND_DISK)

    println("step 3")

    val eventsWithCountries =
      geoData
        .join(maskedIpEvents)
        .where($"mask_size" === $"network_size" and $"network_ip" === $"masked_ip")
        .checkpoint()
        .persist(StorageLevel.MEMORY_AND_DISK)

    println("step 4")

    val q = eventsWithCountries
      .groupBy("country_name").agg(sum("product_price")
      .as("money_spent")).orderBy("money_spent").take(10)


    println("final result*: \n")

    // output
    println(q.take(100).mkString("\n"))

    geoData.unpersist()
    networkSizes.unpersist()
    maskedIpEvents.unpersist()
    eventsWithCountries.unpersist()
  }

  // ------ SPARK RDD -----

  def mostPurchasedCategoriesRDD(spark: SparkSession, tableName: String) = {
    import spark.implicits._

    val events = SparkIO.loadEvents(spark, eventsRoot).as[Purchase].rdd
    val ordering = new Ordering[CategoryAndProductsAmount] {
      override def compare(x: CategoryAndProductsAmount, y: CategoryAndProductsAmount): Int = y.products_sold.compare(x.products_sold)
    }

    val r = events
      .keyBy(_.product_category)
      .aggregateByKey(0)((count, _) => count + 1, _ + _)
      .map(x => CategoryAndProductsAmount(x._1, x._2))
      .takeOrdered(10)(ordering)

    setOptions(r.toSeq.toDS().write, tableName).mode(SaveMode.Overwrite).save()
  }

  def mostPurchasedProductsPerCategoryRDD(spark: SparkSession, tableName: String) = {
    import spark.implicits._

    val events = SparkIO.loadEvents(spark, eventsRoot).as[Purchase].rdd

    val r = events
      .keyBy(x => (x.product_category, x.product_name)).mapValues(_ => 1)
      .repartitionAndSortWithinPartitions(new MyPartitioner(4, _.asInstanceOf[(String, String)]._1.hashCode))
      .mapPartitions(it => new GroupingIterator(it))
      .keyBy(x => (x._1._1, -x._2))
      .mapValues(x => x._1._2)
      .repartitionAndSortWithinPartitions(new MyPartitioner(4, _.asInstanceOf[(String, Int)]._1.hashCode))
      .mapPartitions(_.take(10))
      .map(kvs => CategoryProductAndRevenue(kvs._1._1, kvs._2, -kvs._1._2))

    // output
    setOptions(r.toDS().write, tableName).mode(SaveMode.Overwrite).save()
  }

  def mostSpendingCountriesRDD(spark: SparkSession, tableName: String) = {
    import spark.implicits._

    val geoIps = SparkIO.loadCityGeo(spark, hdfsGeoIps).rdd.map(row => IdNetwork(row.getAs[String](1), row.getAs[String](0)))
    val countryNetworks = SparkIO.loadCountryNetworks(spark, hdfsCountryNetworks).rdd.map(row => IdCountryName(row.getAs[String](0), row.getAs[String](5)))
    val events = SparkIO.loadEvents(spark, eventsRoot).as[Purchase].rdd

    val countiesNetworks = geoIps.keyBy(_.geoname_id)
      .join(countryNetworks.keyBy(_.geoname_id))
      .map { case (_, (idNetwork, idCountryName)) => IpFormatter.ipToMaskAndNetwork(idNetwork.network) -> idCountryName.country_name }
      .persist(StorageLevel.MEMORY_AND_DISK)

    val ipWithCountries: RDD[((Int, Int), (Purchase, String))] = events.flatMap { x =>
      val ip = IpFormatter.ipToInt(x.client_ip)
      (1 to 32).map(ip -> _).map(_ -> x)
    }.join(countiesNetworks)

    val r = ipWithCountries
      .keyBy(x => x._2._2)
      .mapValues(_._2._1.product_price)
      .aggregateByKey(0.0)(_ + _, _ + _)
      .map(x => CountryAndRevenue(x._1, x._2))
      .sortBy(_.revenue)
      .take(10)

    // output
    setOptions(r.toSeq.toDS().write, tableName).mode(SaveMode.Overwrite).save()
  }
}


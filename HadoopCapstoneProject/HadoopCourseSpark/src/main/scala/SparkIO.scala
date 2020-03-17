import org.apache.hadoop.fs.{LocatedFileStatus, RemoteIterator}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrameWriter, Dataset, Encoder, SparkSession}
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}

trait SparkIO[M[_]] {
  def loadEvents(hdfsEventsRoot: String)(implicit spark: SparkSession): M[Purchase]

  def loadCityGeo(hdfsGeoIps: String)(implicit spark: SparkSession): M[IdNetwork]

  def loadCountryNetworks(hdfsCountryNetworks: String)(implicit spark: SparkSession): M[IdCountryName]

  def getWriter[T : Encoder](data: M[T])(implicit sparkSession: SparkSession): DataFrameWriter[T]
}

object SparkRddIO extends SparkIO[RDD]{
  def loadEvents(hdfsEventsRoot: String)(implicit spark: SparkSession): RDD[Purchase] =
    SparkDatasetIO.loadEvents(hdfsEventsRoot).rdd

  def loadCityGeo(hdfsGeoIps: String)(implicit spark: SparkSession): RDD[IdNetwork] =
    SparkDatasetIO.loadCityGeo(hdfsGeoIps).rdd

  def loadCountryNetworks(hdfsCountryNetworks: String)(implicit spark: SparkSession): RDD[IdCountryName] =
    SparkDatasetIO.loadCountryNetworks(hdfsCountryNetworks).rdd

  def getWriter[T : Encoder](data: RDD[T])(implicit sparkSession: SparkSession): DataFrameWriter[T] = {
    import sparkSession.implicits._
    data.toDS().write
  }
}

object SparkDatasetIO extends Serializable with SparkIO[Dataset] {
  import org.apache.hadoop.fs.{FileSystem, Path}

  def loadEvents(hdfsEventsRoot: String)(implicit spark: SparkSession): Dataset[Purchase] = {
    import spark.implicits._
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
    spark.read.schema(schema).csv(paths: _*).as[Purchase]
  }

  def loadCityGeo(hdfsGeoIps: String)(implicit spark: SparkSession): Dataset[IdNetwork] = {
    import spark.implicits._
    spark.read.option("header", "true").csv(hdfsGeoIps).as[IdNetwork]
  }

  def loadCountryNetworks(hdfsCountryNetworks: String)(implicit spark: SparkSession): Dataset[IdCountryName] = {
    import spark.implicits._
    spark.read.option("header", "true").csv(hdfsCountryNetworks).as[IdCountryName]
  }

  def getWriter[T : Encoder](data: Dataset[T])(implicit sparkSession: SparkSession): DataFrameWriter[T] = data.write
}

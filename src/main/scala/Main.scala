import org.apache.iceberg.SortOrder
import org.apache.iceberg.spark.Spark3Util
import org.apache.iceberg.spark.actions.SparkActions
import org.apache.spark.sql.SparkSession

object Main {
  private val nycRawTable = "nyc.raw"

  def main(args: Array[String]): Unit = {
    val spark = YellowSparkSession.spark

    compactFiles(spark, nycRawTable)
    expireSnapshots(spark, nycRawTable)
    rewriteManifests(spark, nycRawTable)
  }

  private def compactFiles(spark: SparkSession, table: String): Unit = {
    val icebergTable = Spark3Util.loadIcebergTable(spark, table)
    val sortOrder = SortOrder.builderFor(icebergTable.schema()).asc("id").build()

    SparkActions.get(spark)
      .rewriteDataFiles(icebergTable)
      .option("target-file-size-bytes", (1024L * 1024L * 256L).toString)
      .option("max-concurrent-file-group-rewrites", (1L).toString)
      .option("min-input-files", (1L).toString)
      .sort(sortOrder)
      .execute
  }

  private def expireSnapshots(spark: SparkSession, table: String): Unit = {
    val icebergTable = Spark3Util.loadIcebergTable(spark, table)
    val tsToExpire = System.currentTimeMillis() - (1000 * 60 * 60)

    SparkActions.get(spark)
      .expireSnapshots(icebergTable)
      .expireOlderThan(tsToExpire)
      .execute
  }

  private def rewriteManifests(spark: SparkSession, table: String): Unit = {
    val icebergTable = Spark3Util.loadIcebergTable(spark, table)

    SparkActions
      .get(spark)
      .rewriteManifests(icebergTable)
      .execute
  }
}

object YellowSparkSession {
  lazy val spark: SparkSession = SparkSession.builder()
    .master("spark://spark-iceberg:7077")
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    .config("spark.hadoop.fs.s3a.access.key", "minio")
    .config("spark.hadoop.fs.s3a.secret.key", "minio123")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.warehouse.dir", "s3a://local-warehouse/spark-warehouse")
    .config("spark.sql.defaultCatalog", "iceberg")
    .config("spark.sql.catalogImplementation", "in-memory")
    .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.iceberg.type", "hive")
    .config("spark.sql.catalog.iceberg.uri", "thrift://hive-metastore:9083")
    .appName("Main")
    .getOrCreate()
}
package org.example

import org.apache.logging.log4j.{LogManager, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import scala.collection.JavaConversions._
import org.apache.spark.sql.SaveMode._
import org.apache.hudi.DataSourceReadOptions._
import org.apache.hudi.DataSourceWriteOptions._
import org.apache.hudi.common.table.HoodieTableConfig._
import org.apache.hudi.config.HoodieWriteConfig._
import org.apache.hudi.keygen.constant.KeyGeneratorOptions._
import org.apache.hudi.common.model.HoodieRecord


object App {
  val logger: Logger = LogManager.getLogger(App.getClass)
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "C:\\hadoop\\") // Thay đổi đường dẫn nếu cần


    // Cấu hình Spark
    val conf = new SparkConf()
      .setAppName("my app")
      .setMaster("local[*]")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog")
      .set("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionCatalog")
      .set("spark.hadoop.fs.s3a.access.key", "minioadmin")
      .set("spark.hadoop.fs.s3a.secret.key", "minioadmin")
      .set("spark.hadoop.fs.s3a.endpoint", "http://127.0.0.1:9000")
      .set("spark.hadoop.fs.s3a.path.style.access", "true")


    val sc = new SparkContext(conf)
    val spark = SparkSession.builder.config(conf).getOrCreate()

    // Đọc file từ S3
    logger.info("Bắt đầu đọc file từ S3...")
    val dataPath = "s3a://openlake/taxi-data.csv"
    val rdd1: DataFrame = spark.read.option("header", "true").option("inferSchema", "true").csv(dataPath)

    rdd1.printSchema()
    rdd1.show(10)

    val hudiTablePath = "s3a://openlake/hudi/tables/hudi_trips_table"
    val tableName = "hudi_trips_table"
    val basePath = "s3a://openlake/hudi/tables/hudi_trips_table"


    rdd1.write.format("hudi").
      option("hoodie.datasource.write.partitionpath.field", "rate_code").
      option("hoodie.table.name", tableName).
      mode(Overwrite).
      save(basePath)
    println(s"Hudi table written to S3 at: $hudiTablePath")

    val hudiDf: DataFrame = spark.read
      .format("hudi")
      .load(hudiTablePath)

    // Hiển thị dữ liệu
    hudiDf.show(10)
    hudiDf.printSchema()

  }

}

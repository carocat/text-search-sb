package com.sb.search.model

import com.google.common.io.Files
import com.typesafe.config.ConfigFactory
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.mockito.MockitoAnnotations
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.must.Matchers

import scala.collection.convert.ImplicitConversions.`set asScala`

trait SparkBaseTest extends AnyFunSuite with Matchers with BeforeAndAfterAll {
  self =>

  implicit var sparkSession: SparkSession = _

  override def beforeAll() {
    super.beforeAll()
    MockitoAnnotations.initMocks(this)

    val conf = new SparkConf()
      .setAll(ConfigFactory.load().entrySet().toList.map(kv => (kv.getKey, kv.getValue.unwrapped().toString)))


    sparkSession = SparkSession
      .builder()
      .master("local[*]")
      .config(conf)
      .config("spark.driver.bindAddress", "127.0.0.1")
      .getOrCreate

    sparkSession.sparkContext.setLogLevel("WARN") // Reduce spam in test logs
    sparkSession.sparkContext.setCheckpointDir(Files.createTempDir().getAbsolutePath)

  }

  def assertDatasetEquals(expected: Dataset[Row], result: Dataset[Row]): Unit = {
    val rowsThatDiffer = result.union(expected).except(result.intersect(expected)).count
    assert(rowsThatDiffer == 0, s"Number of rows that differ: $rowsThatDiffer")
  }

  override def afterAll() {
    super.afterAll()
    sparkSession.stop()
  }

}

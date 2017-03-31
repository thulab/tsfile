package com.corp.delta.tsfile

import java.io.File
import com.corp.delta.tsfile.read.qp.SQLConstant
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.junit.Assert
import org.scalatest.{BeforeAndAfterAll, FunSuite}

/**
  * @author QJL
  */
class TSFileSuit extends FunSuite with BeforeAndAfterAll {

  private val csvPath: String = "src/test/resources/test.csv"
  private val tsfilePath: String = "src/test/resources/test.tsfile"
  private var spark: SparkSession = _

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    new CreateTSFile().createTSFile(csvPath, tsfilePath)
    spark = SparkSession
      .builder()
      .config("spark.master", "local")
      .appName("TSFile test")
      .getOrCreate()
  }

  override protected def afterAll(): Unit = {
    val csvFile = new File(csvPath)
    val tsFile = new File(tsfilePath)
    if (csvFile.exists) csvFile.delete
    if (tsFile.exists) tsFile.delete
    try {
      spark.sparkContext.stop()
    } finally {
      super.afterAll()
    }
  }

  test("testCount") {
    val df = spark.read.tsfile(tsfilePath)
    df.createOrReplaceTempView("tsfile_table")
    val newDf = spark.sql("select count(*) from tsfile_table")

    Assert.assertEquals(30, newDf.head().apply(0).asInstanceOf[Long])
  }

  test("testSelect *") {
    val df = spark.read.tsfile(tsfilePath)
    df.createOrReplaceTempView("tsfile_table")
    val newDf = spark.sql("select * from tsfile_table").cache()

    val count = newDf.count()
    Assert.assertEquals(30, count)
  }

  test("testQueryData") {
    val df = spark.read.tsfile(tsfilePath)
    df.createOrReplaceTempView("tsfile_table")

    val newDf = spark.sql("select * from tsfile_table where s1 > 30").cache()
    val count = newDf.count()

    Assert.assertEquals(14, count)
  }

  test("testQueryDataComplex1") {
    val df = spark.read.tsfile(tsfilePath)
    df.createOrReplaceTempView("tsfile_table")

    val newDf = spark.sql("select * from tsfile_table where s1 > 30 and delta_object = 'root.car.d1'").cache()
    val count = newDf.count()
    Assert.assertEquals(7, count)
  }

  test("testQueryDataComplex2") {
    val df = spark.read.tsfile(tsfilePath)
    df.createOrReplaceTempView("tsfile_table")

    val newDf = spark.sql("select * from tsfile_table where s1 > 30 and delta_object = 'root.car.d1' or s2 > 30 and delta_object = 'root.car.d2'").cache()
    val count = newDf.count()
    Assert.assertEquals(15, count)
  }

  test("testQuerySchema") {
    val df = spark.read.tsfile(tsfilePath)

    val expected = StructType(Seq(
      StructField(SQLConstant.RESERVED_TIME, LongType, nullable = true),
      StructField(SQLConstant.RESERVED_DELTA_OBJECT, StringType, nullable = true),
      StructField("s1", IntegerType, nullable = true),
      StructField("s2", IntegerType, nullable = true)
    ))
    Assert.assertEquals(expected, df.schema)
  }

}
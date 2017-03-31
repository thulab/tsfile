package com.corp.delta.tsfile

import java.util

import com.corp.delta.tsfile.common.utils.TSRandomAccessFileReader
import com.corp.delta.tsfile.file.metadata.enums.TSDataType
import com.corp.delta.tsfile.query.QueryProcessor
import com.corp.delta.tsfile.query.common.{BasicOperator, FilterOperator, TSQueryPlan}
import com.corp.delta.tsfile.read.metadata.SeriesSchema
import com.corp.delta.tsfile.read.qp.SQLConstant
import com.corp.delta.tsfile.read.query.QueryConfig
import com.corp.delta.tsfile.read.readSupport.Field
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._

import scala.collection.JavaConversions._
import scala.collection.mutable.{ArrayBuffer, ListBuffer}

/**
  * This object contains methods that are used to convert schema and data between sparkSQL and TSFile.
  *
  * @author QJL
  */
object Converter {

  class SparkSqlFilterException (message: String, cause: Throwable)
    extends Exception(message, cause){
    def this(message: String) = this(message, null)
  }

  case class SchemaType(dataType: DataType, nullable: Boolean)

  /**
    * Convert TSFile columns to sparkSQL schema.
    *
    * @param tsfileSchema all time series information in TSFile
    * @return sparkSQL table schema
    */
  def toSparkSqlSchema(tsfileSchema: util.ArrayList[SeriesSchema]): Option[StructType] = {
    val fields = new ListBuffer[StructField]()
    fields += StructField(SQLConstant.RESERVED_TIME, LongType, nullable = false)
    fields += StructField(SQLConstant.RESERVED_DELTA_OBJECT, StringType, nullable = false)

    tsfileSchema.foreach((series: SeriesSchema) => {
      fields += StructField(series.name, series.dataType match {
        case TSDataType.BOOLEAN => BooleanType
        case TSDataType.INT32 => IntegerType
        case TSDataType.INT64 => LongType
        case TSDataType.FLOAT => FloatType
        case TSDataType.DOUBLE => DoubleType
        case TSDataType.ENUMS => StringType
        case TSDataType.BYTE_ARRAY => BinaryType
        case TSDataType.FIXED_LEN_BYTE_ARRAY => BinaryType
        case other => throw new UnsupportedOperationException(s"Unsupported type $other")
      }, nullable = true)
    })

    SchemaType(StructType(fields.toList), nullable = false).dataType match {
      case t: StructType => Some(t)
      case _ =>throw new RuntimeException(
        s"""TSFile schema cannot be converted to a Spark SQL StructType:
            |${tsfileSchema.toString}
            |""".stripMargin)
    }
  }

  /**
    * Use information given by sparkSQL to construct TSFile QueryConfigs for querying data.
    *
    * @param in file input stream
    * @param requiredSchema The schema of the data that should be output for each row.
    * @param filters A set of filters than can optionally be used to reduce the number of rows output
    * @param start the start offset in file partition
    * @param end the end offset in file partition
    * @return TSFile physical query plans
    */
  def toQueryConfigs(in: TSRandomAccessFileReader, requiredSchema: StructType, filters: Seq[Filter], start : java.lang.Long, end : java.lang.Long): Array[QueryConfig] = {

    val queryConfigs = new ArrayBuffer[QueryConfig]()

    val paths = new ListBuffer[String]()
    requiredSchema.foreach(f => {
      paths.add(f.name)
    })

    //remove invalid filters
    val validFilters = new ListBuffer[Filter]()
    filters.foreach {f => {
      if(isValidFIlter(f))
        validFilters.add(f)}
    }

    if(validFilters.isEmpty) {

      //generatePlans operatorTree to TSQueryPlan list
      val queryProcessor = new QueryProcessor()
      val tsfileQuerys = queryProcessor.generatePlans(null, paths, in, start, end).toArray

      //construct TSQueryPlan list to QueryConfig list
      tsfileQuerys.foreach(f => {
        queryConfigs.append(queryToConfig(f.asInstanceOf[TSQueryPlan]))
      })
    } else {
      //construct filters to a binary tree
      var filterTree = validFilters.get(0)
      for(i <- 1 until validFilters.length) {
        filterTree = And(filterTree, validFilters.get(i))
      }

      //convert filterTree to FilterOperator
      val operator = transformFilter(filterTree)

      //generatePlans operatorTree to TSQueryPlan list
      val queryProcessor = new QueryProcessor()
      val queryPlans = queryProcessor.generatePlans(operator, paths, in, start, end).toArray

      //construct TSQueryPlan list to QueryConfig list
      queryPlans.foreach(f => {
        queryConfigs.append(queryToConfig(f.asInstanceOf[TSQueryPlan]))
      })
    }
    queryConfigs.toArray
  }

  private def isValidFIlter(filter: Filter): Boolean = {
    filter match {
      case f: EqualTo => true
      case f: GreaterThan => true
      case f: GreaterThanOrEqual => true
      case f: LessThan => true
      case f: LessThanOrEqual => true
      case f: Or => isValidFIlter(f.left) && isValidFIlter(f.right)
      case f: And => isValidFIlter(f.left) && isValidFIlter(f.right)
      case f: Not => isValidFIlter(f.child)
      case _ => false
    }
  }

  /**
    * Used in toQueryConfigs() to convert one query plan to one QueryConfig.
    *
    * @param queryPlan TSFile logical query plan
    * @return TSFile physical query plan
    */
  private def queryToConfig(queryPlan: TSQueryPlan): QueryConfig = {
    val selectedColumns = queryPlan.getPaths.toArray
    val timeFilter = queryPlan.getTimeFilterOperator
    val valueFilter = queryPlan.getValueFilterOperator

    var select = ""
    var colNum = 0
    selectedColumns.foreach(f => {
      if(colNum == 0) {
        select += f.asInstanceOf[String]
      }
      else {
        select += "|" + f.asInstanceOf[String]
      }
      colNum += 1
    })

    var single = false
    if(colNum == 1 && valueFilter != null){
      if(select.contentEquals(valueFilter.getSinglePath)){
        single = true
      }
    }
    val timeFilterStr = timeFilterToString(timeFilter)
    val valueFilterStr = valueFilterToString(valueFilter, single)
    new QueryConfig(select, timeFilterStr, "null", valueFilterStr)
  }


  /**
    * Convert a time filter to QueryConfig's timeFilter parameter.
    *
    * @param operator time filter
    * @return QueryConfig's timeFilter parameter
    */
  private def timeFilterToString(operator: FilterOperator): String = {
    if(operator == null)
      return "null"

    "0," + timeFilterToPartString(operator)
  }


  /**
    * Used in timeFilterToString to construct specified string format.
    *
    * @param operator time filter
    * @return QueryConfig's partial timeFilter parameter
    */
  private def timeFilterToPartString(operator: FilterOperator): String = {
    val token = operator.getTokenIntType
    token match {
      case SQLConstant.KW_AND =>
        "(" + timeFilterToPartString(operator.getChildren()(0)) + ")&(" +
          timeFilterToPartString(operator.getChildren()(1)) + ")"
      case SQLConstant.KW_OR =>
        "(" + timeFilterToPartString(operator.getChildren()(0)) + ")|(" +
          timeFilterToPartString(operator.getChildren()(1)) + ")"
      case _ =>
        val basicOperator = operator.asInstanceOf[BasicOperator]
        basicOperator.getTokenSymbol + basicOperator.getSeriesValue
    }
  }


  /**
    * Convert a value filter to QueryConfig's valueFilter parameter. Each query is a cross query.
    *
    * @param operator value filter
    * @param single single series query
    * @return QueryConfig's valueFilter parameter
    */
  private def valueFilterToString(operator: FilterOperator, single : Boolean): String = {
    if(operator == null)
      return "null"

    val token = operator.getTokenIntType
    token match {
      case SQLConstant.KW_AND =>
        "[" + valueFilterToString(operator.getChildren()(0), single = true) + "]&[" +
          valueFilterToString(operator.getChildren()(1), single = true) + "]"
      case SQLConstant.KW_OR =>
        "[" + valueFilterToString(operator.getChildren()(0), single = true) + "]|[" +
          valueFilterToString(operator.getChildren()(1), single = true) + "]"
      case _ =>
        val basicOperator = operator.asInstanceOf[BasicOperator]
        val path = basicOperator.getSinglePath
        val res = new StringBuilder
        if(single){
          res.append("2," + path + "," +
            basicOperator.getTokenSymbol + basicOperator.getSeriesValue)
        }
        else{
          res.append("[2," + path + "," +
            basicOperator.getTokenSymbol + basicOperator.getSeriesValue + "]")
        }
        res.toString()
    }
  }


  /**
    * Transform sparkSQL's filter binary tree to filterOperator binary tree.
    *
    * @param node filter tree's node
    * @return TSFile filterOperator binary tree
    */
  private def transformFilter(node: Filter): FilterOperator = {
    var operator: FilterOperator = null
    node match {
      case node: Not =>
        operator = new FilterOperator(SQLConstant.KW_NOT)
        operator.addChildOPerator(transformFilter(node.child))
        operator

      case node: And =>
        operator = new FilterOperator(SQLConstant.KW_AND)
        operator.addChildOPerator(transformFilter(node.left))
        operator.addChildOPerator(transformFilter(node.right))
        operator

      case node: Or =>
        operator = new FilterOperator(SQLConstant.KW_OR)
        operator.addChildOPerator(transformFilter(node.left))
        operator.addChildOPerator(transformFilter(node.right))
        operator

      case node: EqualTo =>
        operator = new BasicOperator(SQLConstant.EQUAL, node.attribute, node.value.toString)
        operator

      case node: LessThan =>
        operator = new BasicOperator(SQLConstant.LESSTHAN, node.attribute, node.value.toString)
        operator

      case node: LessThanOrEqual =>
        operator = new BasicOperator(SQLConstant.LESSTHANOREQUALTO, node.attribute, node.value.toString)
        operator

      case node: GreaterThan =>
        operator = new BasicOperator(SQLConstant.GREATERTHAN, node.attribute, node.value.toString)
        operator

      case node: GreaterThanOrEqual =>
        operator = new BasicOperator(SQLConstant.GREATERTHANOREQUALTO, node.attribute, node.value.toString)
        operator

      case _ =>
        throw new Exception("unsupported filter:" + node.toString)
    }
  }


  /**
    * Convert TSFile data to sparkSQL data.
    *
    * @param field TSFile's one data point
    * @return sparkSQL's one data point in one column
    */
  def toSqlValue(field: Field): Any = {
    if (field.isNull)
      null
    else field.dataType match {
      case TSDataType.BOOLEAN => field.getBoolV
      case TSDataType.INT32 => field.getIntV
      case TSDataType.INT64 => field.getLongV
      case TSDataType.FLOAT => field.getFloatV
      case TSDataType.DOUBLE => field.getDoubleV
      case TSDataType.FIXED_LEN_BYTE_ARRAY => field.getStringValue
      case TSDataType.BYTE_ARRAY => field.getBinaryV.values
      case TSDataType.ENUMS => field.getStringValue
      case other => throw new UnsupportedOperationException(s"Unsupported type $other")
    }
  }
}

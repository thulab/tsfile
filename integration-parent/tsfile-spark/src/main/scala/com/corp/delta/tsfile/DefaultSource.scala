package com.corp.delta.tsfile

import java.io._
import java.net.URI
import java.util

import DefaultSource.SerializableConfiguration
import com.corp.delta.tsfile.common.constant.QueryConstant
import com.corp.delta.tsfile.hadoop.io.HDFSInputStream
import com.corp.delta.tsfile.query.Executor
import com.corp.delta.tsfile.read.qp.SQLConstant
import com.corp.delta.tsfile.read.query.{QueryDataSet, QueryEngine}
import com.corp.delta.tsfile.read.readSupport.{Field, RowRecord}

import com.esotericsoftware.kryo.io.{Input, Output}
import com.esotericsoftware.kryo.{Kryo, KryoSerializable}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.hadoop.mapreduce.Job

import org.apache.spark.TaskContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.apache.spark.sql.execution.datasources.{FileFormat, OutputWriterFactory, PartitionedFile}
import org.apache.spark.sql.sources.{DataSourceRegister, Filter}
import org.apache.spark.sql.types.{StructField, StructType}

import org.slf4j.{Logger, LoggerFactory}

import collection.JavaConversions._
import scala.collection.mutable

/**
  * TSFile data source
  *
  * @author QJL
  * @author MXW
  */
private[tsfile] class DefaultSource extends FileFormat with DataSourceRegister {

  class TSFileDataSourceException(message: String, cause: Throwable)
    extends Exception(message, cause){
    def this(message: String) = this(message, null)
  }

  override def equals(other: Any): Boolean = other match {
    case _: DefaultSource => true
    case _ => false
  }

  override def inferSchema(
                            spark: SparkSession,
                            options: Map[String, String],
                            files: Seq[FileStatus]): Option[StructType] = {

    val conf = spark.sparkContext.hadoopConfiguration

    //check if the path is given
    options.getOrElse(DefaultSource.path, throw new TSFileDataSourceException(s"${DefaultSource.path} must be specified for tsfile DataSource"))

    val sampleFile = files.find(f => true).getOrElse {
      throw new FileNotFoundException("No tsfile found.")
    }

    //get all series in tsfile
    val tsfileSchema = {
      val in = new HDFSInputStream(sampleFile.getPath, conf)
      val queryEngine = new QueryEngine(in)
      queryEngine.getAllSeries
    }

    Converter.toSparkSqlSchema(tsfileSchema)
  }

  override def isSplitable(
                            sparkSession: SparkSession,
                            options: Map[String, String],
                            path: Path): Boolean = {
    true
  }

  override def buildReader(
                            sparkSession: SparkSession,
                            dataSchema: StructType,
                            partitionSchema: StructType,
                            requiredSchema: StructType,
                            filters: Seq[Filter],
                            options: Map[String, String],
                            hadoopConf: Configuration): (PartitionedFile) => Iterator[InternalRow] = {
    val broadcastedConf =
      sparkSession.sparkContext.broadcast(new SerializableConfiguration(hadoopConf))

    (file: PartitionedFile) => {

      val conf = broadcastedConf.value.value
      val in = new HDFSInputStream(new Path(new URI(file.filePath)), conf)

      var taskInfo: String = ""
      Option(TaskContext.get()).foreach { taskContext => {
        taskContext.addTaskCompletionListener { _ => in.close() }
        taskInfo += "task Id: " + taskContext.taskAttemptId() + " partition Id: " + taskContext.partitionId()}
      }
      DefaultSource.logger.debug("taskInfo: {}", taskInfo)

      val parameters = new util.HashMap[java.lang.String, java.lang.Long]()
      parameters.put(QueryConstant.PARTITION_START_OFFSET, file.start.asInstanceOf[java.lang.Long])
      parameters.put(QueryConstant.PARTITION_END_OFFSET, (file.start + file.length).asInstanceOf[java.lang.Long])

      //convert tsfilequery to QueryConfigs
      val queryConfigs = Converter.toQueryConfigs(in, requiredSchema, filters, file.start.asInstanceOf[java.lang.Long], (file.start + file.length).asInstanceOf[java.lang.Long])

      //use QueryConfigs to query tsfile
      val dataSets = Executor.query(in, queryConfigs.toList, parameters)

      case class Record(record: RowRecord, index: Int)

      implicit object RowRecordOrdering extends Ordering[Record] {
        override def compare(r1: Record, r2: Record): Int = {
          if(r1.record.timestamp == r2.record.timestamp) {
            r1.record.getFields.get(0).deltaObjectId.compareTo(r2.record.getFields.get(0).deltaObjectId)
          } else if(r1.record.timestamp < r2.record.timestamp){
            1
          } else {
            -1
          }
        }
      }

      val priorityQueue = new mutable.PriorityQueue[Record]()

      //init priorityQueue with first record of each dataSet
      var queryDataSet: QueryDataSet = null
      for(i <- 0 until dataSets.size()) {
        queryDataSet = dataSets(i)
        if(queryDataSet.hasNextRecord) {
          val rowRecord = queryDataSet.getNextRecord
          priorityQueue.enqueue(Record(rowRecord, i))
        }
      }

      var curRecord: Record = null

      new Iterator[InternalRow] {
        private val rowBuffer = Array.fill[Any](requiredSchema.length)(null)

        private val safeDataRow = new GenericRow(rowBuffer)

        // Used to convert `Row`s containing data columns into `InternalRow`s.
        private val encoderForDataColumns = RowEncoder(requiredSchema)

        override def hasNext: Boolean = {
          var hasNext = false

          while (priorityQueue.nonEmpty && !hasNext) {
            //get a record from priorityQueue
            var tmpRecord = priorityQueue.dequeue()
            //insert a record to priorityQueue
            if (dataSets(tmpRecord.index).hasNextRecord) {
              priorityQueue.enqueue(Record(dataSets(tmpRecord.index).getNextRecord, tmpRecord.index))
            }

            if (curRecord == null || tmpRecord.record.timestamp != curRecord.record.timestamp ||
              !tmpRecord.record.getFields.get(0).deltaObjectId.equals(curRecord.record.getFields.get(0).deltaObjectId)) {
              curRecord = tmpRecord
              hasNext = true
            }
          }

          hasNext
        }

        override def next(): InternalRow = {

          val fields = new scala.collection.mutable.HashMap[String, Field]()
          for (i <- 0 until curRecord.record.fields.size()) {
            val field = curRecord.record.fields.get(i)
            fields.put(field.measurementId, field)
          }

          //index in one required row
          var index = 0
          requiredSchema.foreach((field: StructField) => {
            if (field.name == SQLConstant.RESERVED_TIME) {
              rowBuffer(index) = curRecord.record.timestamp
            } else if (field.name == SQLConstant.RESERVED_DELTA_OBJECT) {
              rowBuffer(index) = curRecord.record.getFields.get(0).deltaObjectId
            } else {
              rowBuffer(index) = Converter.toSqlValue(fields.getOrElse(field.name, null))
            }
            index += 1
          })

          encoderForDataColumns.toRow(safeDataRow)
        }
      }
    }
  }

  override def shortName(): String = "tsfile"

  override def prepareWrite(sparkSession: SparkSession,
                            job: Job, options: Map[String, String],
                            dataSchema: StructType): OutputWriterFactory = {
    null
  }

}


private[tsfile] object DefaultSource {
  val logger: Logger = LoggerFactory.getLogger(getClass)
  val path = "path"

  class SerializableConfiguration(@transient var value: Configuration) extends Serializable with KryoSerializable{
    override def write(kryo: Kryo, output: Output): Unit = {
      val dos = new DataOutputStream(output)
      value.write(dos)
      dos.flush()
    }

    override def read(kryo: Kryo, input: Input): Unit = {
      value = new Configuration(false)
      value.readFields(new DataInputStream(input))
    }
  }

}

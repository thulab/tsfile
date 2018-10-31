package cn.edu.tsinghua.tsfile.timeseries.write;

import cn.edu.tsinghua.tsfile.common.conf.TSFileConfig;
import cn.edu.tsinghua.tsfile.common.conf.TSFileDescriptor;
import cn.edu.tsinghua.tsfile.common.utils.ITsRandomAccessFileWriter;
import cn.edu.tsinghua.tsfile.timeseries.write.desc.MeasurementDescriptor;
import cn.edu.tsinghua.tsfile.timeseries.write.exception.NoMeasurementException;
import cn.edu.tsinghua.tsfile.timeseries.write.exception.WriteProcessException;
import cn.edu.tsinghua.tsfile.timeseries.write.io.TsFileIOWriter;
import cn.edu.tsinghua.tsfile.timeseries.write.record.DataPoint;
import cn.edu.tsinghua.tsfile.timeseries.write.record.TSRecord;
import cn.edu.tsinghua.tsfile.timeseries.write.schema.FileSchema;
import cn.edu.tsinghua.tsfile.timeseries.write.schema.converter.JsonConverter;
import cn.edu.tsinghua.tsfile.timeseries.write.series.IRowGroupWriter;
import cn.edu.tsinghua.tsfile.timeseries.write.series.RowGroupWriterImpl;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * TsFileWriter is the entrance for writing processing. It receives a record and send it to
 * responding row group write. It checks memory size for all writing processing along its strategy
 * and flush data stored in memory to OutputStream. At the end of writing, user should call
 * {@code close()} method to flush the last data outside and close the normal outputStream and error
 * outputStream.
 *
 * @author kangrong
 */
public class TsFileWriter {
  private static final Logger LOG = LoggerFactory.getLogger(TsFileWriter.class);
  /** min value of threshold of data points num check **/
  private static final int MINIMUM_RECORD_COUNT_FOR_CHECK = 1;
  /** IO writer of this tsfile **/
  protected final TsFileIOWriter deltaFileWriter;
  /** schema of this tsfile **/
  protected final FileSchema schema;
  protected final int pageSize;
  /** total byte size of one RowGroup **/
  protected final long primaryRowGroupSize;
  protected long recordCount = 0;
  /** all IRowGroupWriters **/
  protected Map<String, IRowGroupWriter> groupWriters = new HashMap<String, IRowGroupWriter>();
  private long recordCountForNextMemCheck = MINIMUM_RECORD_COUNT_FOR_CHECK;
  private long rowGroupSizeThreshold;
  /** max byte size of one TSRecord **/
  private int oneRowMaxSize;

  /**
   * init this TsFileWriter
   * @param file the File to be written by this TsFileWriter
   * @throws WriteProcessException
   * @throws IOException
   */
  public TsFileWriter(File file) throws WriteProcessException, IOException {
    this(new TsFileIOWriter(file), new FileSchema(), TSFileDescriptor.getInstance().getConfig());
  }

  /**
   * init this TsFileWriter
   * @param file the File to be written by this TsFileWriter
   * @param schema the schema of this tsfile
   * @throws WriteProcessException
   * @throws IOException
   */
  public TsFileWriter(File file, FileSchema schema) throws WriteProcessException, IOException {
    this(new TsFileIOWriter(file), schema, TSFileDescriptor.getInstance().getConfig());
  }

  /**
   * init this TsFileWriter
   * @param file the File to be written by this TsFileWriter
   * @param conf the configuration of this tsfile
   * @throws WriteProcessException
   * @throws IOException
   */
  public TsFileWriter(File file, TSFileConfig conf) throws WriteProcessException, IOException {
    this(new TsFileIOWriter(file), new FileSchema(), conf);
  }

  /**
   * init this TsFileWriter
   * @param file the File to be written by this TsFileWriter
   * @param schema the schema of this tsfile
   * @param conf the configuration of this tsfile
   * @throws WriteProcessException
   * @throws IOException
   */
  public TsFileWriter(File file, FileSchema schema, TSFileConfig conf)
      throws WriteProcessException, IOException {
    this(new TsFileIOWriter(file), schema, conf);
  }

  /**
   * init this TsFileWriter
   * @param output the file writer of this TsFileWriter
   * @throws WriteProcessException
   * @throws IOException
   */
  public TsFileWriter(ITsRandomAccessFileWriter output) throws WriteProcessException, IOException {
    this(new TsFileIOWriter(output), new FileSchema(), TSFileDescriptor.getInstance().getConfig());
  }

  /**
   * init this TsFileWriter
   * @param output the file writer of this TsFileWriter
   * @param schema the schema of this tsfile
   * @throws WriteProcessException
   * @throws IOException
   */
  public TsFileWriter(ITsRandomAccessFileWriter output, FileSchema schema)
      throws WriteProcessException, IOException {
    this(new TsFileIOWriter(output), schema, TSFileDescriptor.getInstance().getConfig());
  }

  /**
   * init this TsFileWriter
   * @param output the file writer of this TsFileWriter
   * @param conf the configuration of this tsfile
   * @throws WriteProcessException
   * @throws IOException
   */
  public TsFileWriter(ITsRandomAccessFileWriter output, TSFileConfig conf)
      throws WriteProcessException, IOException {
    this(new TsFileIOWriter(output), new FileSchema(), conf);
  }

  /**
   * init this TsFileWriter
   * @param output the file writer of this TsFileWriter
   * @param schema the schema of this tsfile
   * @param conf the configuration of this tsfile
   * @throws WriteProcessException
   * @throws IOException
   */
  public TsFileWriter(ITsRandomAccessFileWriter output, FileSchema schema, TSFileConfig conf)
      throws WriteProcessException, IOException {
    this(new TsFileIOWriter(output), schema, conf);
  }

  /**
   * init this TsFileWriter
   * @param tsfileWriter the io writer of this tsfile
   * @param schema the schema of this tsfile
   * @param conf the configuration of this tsfile
   * @throws WriteProcessException
   */
  protected TsFileWriter(TsFileIOWriter tsfileWriter, FileSchema schema, TSFileConfig conf)
      throws WriteProcessException {
    this.deltaFileWriter = tsfileWriter;
    this.schema = schema;
    this.primaryRowGroupSize = conf.groupSizeInByte;
    this.pageSize = conf.pageSizeInByte;
    this.oneRowMaxSize = schema.getCurrentRowMaxSize();
    if (primaryRowGroupSize <= oneRowMaxSize)
      throw new WriteProcessException(
          "initial measurement error: the potential size of one row is too large");
    this.rowGroupSizeThreshold = primaryRowGroupSize - oneRowMaxSize;
  }

  /**
   * add a MeasurementDescriptor to this tsfile
   * @param measurementDescriptor
   * @throws WriteProcessException
   */
  public void addMeasurement(MeasurementDescriptor measurementDescriptor)
      throws WriteProcessException {
    // check if this MeasurementDescriptor has already existed
    if (schema.hasMeasurement(measurementDescriptor.getMeasurementId()))
      throw new WriteProcessException(
          "given measurement has exists! " + measurementDescriptor.getMeasurementId());
    // rigister this MeasurementDescriptor to the schema of this tsfile
    schema.registerMeasurement(measurementDescriptor);
    // update oneRowMaxSize
    this.oneRowMaxSize = schema.getCurrentRowMaxSize();
    // check oneRowMaxSize
    if (primaryRowGroupSize <= oneRowMaxSize)
      throw new WriteProcessException(
          "add measurement error: the potential size of one row is too large");
    // update threshold of RowGroup
    this.rowGroupSizeThreshold = primaryRowGroupSize - oneRowMaxSize;
    try {
      // check memory size
      checkMemorySize();
    } catch (IOException e) {
      throw new WriteProcessException(e.getMessage());
    }
  }

  /**
   * add a new measurement according to json string.
   * @param measurement
   *          example:
   *          <pre>
     {
            "measurement_id": "sensor_cpu_50",
            "data_type": "INT32",
            "encoding": "RLE"
        }
   *          </pre>
   * 
   * @throws WriteProcessException if the json is illegal or the measurement exists
   */
  public void addMeasurementByJson(JSONObject measurement) throws WriteProcessException {
    addMeasurement(JsonConverter.convertJsonToMeasureMentDescriptor(measurement));
  }

  /**
   * Confirm whether the record is legal. If legal, add it into this RecordWriter.
   *
   * @param record
   *          - a record responding a line
   * @return - whether the record has been added into RecordWriter legally
   * @throws WriteProcessException exception
   */
  protected boolean checkIsTimeSeriesExist(TSRecord record) throws WriteProcessException {
		IRowGroupWriter groupWriter;
		// if not exist, create a new RowGroupWriterImpl, else just get it from groupWriters
		if (!groupWriters.containsKey(record.deltaObjectId)) {
			groupWriter = new RowGroupWriterImpl(record.deltaObjectId);
			groupWriters.put(record.deltaObjectId, groupWriter);
		} else{
			groupWriter = groupWriters.get(record.deltaObjectId);
		}

		// add all SeriesWriter of measurements in this TSRecord to this RowGroupWriter
		Map<String, MeasurementDescriptor> schemaDescriptorMap = schema.getDescriptor();
		for (DataPoint dp : record.dataPointList) {
			String measurementId = dp.getMeasurementId();
			if (schemaDescriptorMap.containsKey(measurementId))
				groupWriter.addSeriesWriter(schemaDescriptorMap.get(measurementId), pageSize);
			else
				throw new NoMeasurementException("input measurement is invalid: " + measurementId);
		}
		return true;
  }

  /**
   * write a record in type of T.
   * 
   * @param record
   *          - record responding a data line
   * @throws IOException
   *           exception in IO
   * @throws WriteProcessException
   *           exception in write process
   * @return true -size of tsfile or metadata reaches the threshold. 
   * false - otherwise
   */
  public boolean write(TSRecord record) throws IOException, WriteProcessException {
    // make sure the RowGroupWriter for this TSRecord exist
    if (checkIsTimeSeriesExist(record)) {
      // get corresponding RowGroupWriter and write this TSRecord
      groupWriters.get(record.deltaObjectId).write(record.time, record.dataPointList);
      ++recordCount;
      return checkMemorySize();
    }
	return false;
  }

  /**
   * calculate total memory size occupied by all RowGroupWriter instances.
   *
   * @return total memory size used
   */
  public long calculateMemSizeForAllGroup() {
    int memTotalSize = 0;
    for (IRowGroupWriter group : groupWriters.values()) {
      memTotalSize += group.updateMaxGroupMemSize();
    }
    return memTotalSize;
  }

  /**
   * check occupied memory size, if it exceeds the rowGroupSize threshold, flush them to given
   * OutputStream.
   *
   * @throws IOException
   *           exception in IO
   * @return true - size of tsfile or metadata reaches the threshold. 
   * false - otherwise
   */
  protected boolean checkMemorySize() throws IOException {
    if (recordCount >= recordCountForNextMemCheck) {
      long memSize = calculateMemSizeForAllGroup();
      if (memSize > rowGroupSizeThreshold) {
        LOG.info("start_write_row_group, memory space occupy:" + memSize);
        recordCountForNextMemCheck = rowGroupSizeThreshold / oneRowMaxSize;
        return flushRowGroup(false);
      } else {
        recordCountForNextMemCheck = recordCount
            + (rowGroupSizeThreshold - memSize) / oneRowMaxSize;
        return false;
      }
    }
    return false;
  }

  /**
   * flush the data in all series writers and their page writers to outputStream.
   * 
   * @param isFillRowGroup
   *          whether to fill RowGroup
   * @throws IOException
   *           exception in IO
   * @return true - size of tsfile or metadata reaches the threshold. 
   * false - otherwise. But this function just return false, the Override of IoTDB may return true.
   */
  protected boolean flushRowGroup(boolean isFillRowGroup) throws IOException {
    // at the present stage, just flush one block
    if (recordCount > 0) {
      // 1. get start offset
      long totalMemStart = deltaFileWriter.getPos();
      // 2. loop all deltaObjectIDs
      for (String deltaObjectId : groupWriters.keySet()) {
        // 2.1 get start offset of this RowGroup
        long memSize = deltaFileWriter.getPos();
        // 2.2 write this RowGroup
        deltaFileWriter.startRowGroup(recordCount, deltaObjectId);
        IRowGroupWriter groupWriter = groupWriters.get(deltaObjectId);
        groupWriter.flushToFileWriter(deltaFileWriter);
        deltaFileWriter.endRowGroup(deltaFileWriter.getPos() - memSize);
      }
      // 3. get byte size of all RowGroups
      long actualTotalRowGroupSize = deltaFileWriter.getPos() - totalMemStart;
      // 4. if needed, fill empty size
      if (isFillRowGroup) {
        fillInRowGroupSize(actualTotalRowGroupSize);
        LOG.info("total row group size:{}, actual:{}, filled:{}", primaryRowGroupSize,
            actualTotalRowGroupSize, primaryRowGroupSize - actualTotalRowGroupSize);
      } else
        LOG.info("total row group size:{}, row group is not filled", actualTotalRowGroupSize);
      LOG.info("write row group end");
      // 5. reset recordCount to 0
      recordCount = 0;
      // 6. reset
      reset();
    }
    return false;
  }

  /**
   * fill empty size fo this RowGroup
   * @param actualRowGroupSize actual byte size of this RowGroup
   * @throws IOException
   */
  protected void fillInRowGroupSize(long actualRowGroupSize) throws IOException {
    if (actualRowGroupSize > primaryRowGroupSize)
      LOG.warn("too large actual row group size!:actual:{},threshold:{}", actualRowGroupSize,
          primaryRowGroupSize);
    deltaFileWriter.fillInRowGroup(primaryRowGroupSize - actualRowGroupSize);
  }

  /**
   * reset all RowGroupWriter
   */
  private void reset() {
    groupWriters.clear();
  }

  /**
   * calling this method to write the last data remaining in memory and close the normal and error
   * OutputStream.
   *
   * @throws IOException
   *           exception in IO
   */
  public void close() throws IOException {
    LOG.info("start close file");
    calculateMemSizeForAllGroup();
    flushRowGroup(false);
    deltaFileWriter.endFile(this.schema);
  }
}

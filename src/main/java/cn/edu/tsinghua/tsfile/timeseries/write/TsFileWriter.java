package cn.edu.tsinghua.tsfile.timeseries.write;

import cn.edu.tsinghua.tsfile.common.conf.TSFileConfig;
import cn.edu.tsinghua.tsfile.common.conf.TSFileDescriptor;
import cn.edu.tsinghua.tsfile.common.utils.ITsRandomAccessFileWriter;
import cn.edu.tsinghua.tsfile.common.utils.Pair;
import cn.edu.tsinghua.tsfile.file.metadata.enums.CompressionTypeName;
import cn.edu.tsinghua.tsfile.timeseries.read.query.DynamicOneColumnData;
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

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
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
  private static final int MINIMUM_RECORD_COUNT_FOR_CHECK = 1;
  protected final TsFileIOWriter deltaFileWriter;
  protected final FileSchema schema;
  protected final int pageSize;
  protected final long primaryRowGroupSize;
  protected long recordCount = 0;
  protected Map<String, IRowGroupWriter> groupWriters = new HashMap<String, IRowGroupWriter>();
  private long recordCountForNextMemCheck = MINIMUM_RECORD_COUNT_FOR_CHECK;
  private long rowGroupSizeThreshold;
  private int oneRowMaxSize;

  public TsFileWriter(File file) throws WriteProcessException, IOException {
    this(new TsFileIOWriter(file), new FileSchema(), TSFileDescriptor.getInstance().getConfig());
  }

  public TsFileWriter(File file, FileSchema schema) throws WriteProcessException, IOException {
    this(new TsFileIOWriter(file), schema, TSFileDescriptor.getInstance().getConfig());
  }

  public TsFileWriter(File file, TSFileConfig conf) throws WriteProcessException, IOException {
    this(new TsFileIOWriter(file), new FileSchema(), conf);
  }

  public TsFileWriter(File file, FileSchema schema, TSFileConfig conf)
      throws WriteProcessException, IOException {
    this(new TsFileIOWriter(file), schema, conf);
  }

  public TsFileWriter(ITsRandomAccessFileWriter output) throws WriteProcessException, IOException {
    this(new TsFileIOWriter(output), new FileSchema(), TSFileDescriptor.getInstance().getConfig());
  }

  public TsFileWriter(ITsRandomAccessFileWriter output, FileSchema schema)
      throws WriteProcessException, IOException {
    this(new TsFileIOWriter(output), schema, TSFileDescriptor.getInstance().getConfig());
  }

  public TsFileWriter(ITsRandomAccessFileWriter output, TSFileConfig conf)
      throws WriteProcessException, IOException {
    this(new TsFileIOWriter(output), new FileSchema(), conf);
  }

  public TsFileWriter(ITsRandomAccessFileWriter output, FileSchema schema, TSFileConfig conf)
      throws WriteProcessException, IOException {
    this(new TsFileIOWriter(output), schema, conf);
  }

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

  public void addMeasurement(MeasurementDescriptor measurementDescriptor)
      throws WriteProcessException {
    if (schema.hasMeasurement(measurementDescriptor.getMeasurementId()))
      throw new WriteProcessException(
          "given measurement has exists! " + measurementDescriptor.getMeasurementId());
    schema.registerMeasurement(measurementDescriptor);
    this.oneRowMaxSize = schema.getCurrentRowMaxSize();
    if (primaryRowGroupSize <= oneRowMaxSize)
      throw new WriteProcessException(
          "add measurement error: the potential size of one row is too large");
    this.rowGroupSizeThreshold = primaryRowGroupSize - oneRowMaxSize;
    try {
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
  protected boolean checkIsDeltaExist(TSRecord record) throws WriteProcessException {
    if (!schema.hasDeltaObject(record.deltaObjectId)) {
      schema.addDeltaObject(record.deltaObjectId);
    }
    addGroupToInternalRecordWriter(record);
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
   */
  public void write(TSRecord record) throws IOException, WriteProcessException {
    if (checkIsDeltaExist(record)) {
      groupWriters.get(record.deltaObjectId).write(record.time, record.dataPointList);
      ++recordCount;
      checkMemorySize();
    }
  }

  /**
   * @see cn.edu.tsinghua.tsfile.timeseries.write.series.IRowGroupWriter#getDataInMemory(String)
   * @param deltaObjectId deltaObject id
   * @param measurementId measurement id
   * @return fist object is the current page data, second object is the all pages which is packaged
   */
  public List<Object> getDataInMemory(String deltaObjectId, String measurementId) {
    if (groupWriters.get(deltaObjectId) == null) {
      DynamicOneColumnData left = null;
      Pair<List<ByteArrayInputStream>, CompressionTypeName> right = null;
      List<Object> result = new ArrayList<>();
      result.add(left);
      result.add(right);
      return result;
    }
    return groupWriters.get(deltaObjectId).getDataInMemory(measurementId);
  }

  /**
   * <b>Note that</b>, before calling this method, all {@code IRowGroupWriter} instance existing in
   * {@code groupWriters} have been reset for next writing stage, thus we don't add new
   * {@code IRowGroupWriter} if its deltaObjectId has existed.
   *
   * @param record TSRecord
   * @throws WriteProcessException - delta object to be add
   */
  protected void addGroupToInternalRecordWriter(TSRecord record) throws WriteProcessException {
    IRowGroupWriter groupWriter;
    if (!groupWriters.containsKey(record.deltaObjectId)) {
      groupWriter = new RowGroupWriterImpl(record.deltaObjectId);
      groupWriters.put(record.deltaObjectId, groupWriter);
    } else
      groupWriter = groupWriters.get(record.deltaObjectId);
    Map<String, MeasurementDescriptor> schemaDescriptorMap = schema.getDescriptor();
    for (DataPoint dp : record.dataPointList) {
      String measurementId = dp.getMeasurementId();
      if (schemaDescriptorMap.containsKey(measurementId))
        groupWriter.addSeriesWriter(schemaDescriptorMap.get(measurementId), pageSize);
      else
        throw new NoMeasurementException("input measurement is invalid: " + measurementId);
    }
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
   */
  protected void checkMemorySize() throws IOException {
    if (recordCount >= recordCountForNextMemCheck) {
      long memSize = calculateMemSizeForAllGroup();
      if (memSize > rowGroupSizeThreshold) {
        LOG.info("start_write_row_group, memory space occupy:" + memSize);
        flushRowGroup(true);
        recordCountForNextMemCheck = rowGroupSizeThreshold / oneRowMaxSize;
      } else {
        recordCountForNextMemCheck = recordCount
            + (rowGroupSizeThreshold - memSize) / oneRowMaxSize;
      }
    }
  }

  /**
   * flush the data in all series writers and their page writers to outputStream.
   * 
   * @param isFillRowGroup
   *          whether to fill RowGroup
   * @throws IOException
   *           exception in IO
   */
  protected void flushRowGroup(boolean isFillRowGroup) throws IOException {
    // at the present stage, just flush one block
    if (recordCount > 0) {
      long totalMemStart = deltaFileWriter.getPos();
      for (String deltaObjectId : schema.getDeltaObjectAppearedSet()) {
        long memSize = deltaFileWriter.getPos();
        deltaFileWriter.startRowGroup(recordCount, deltaObjectId);
        IRowGroupWriter groupWriter = groupWriters.get(deltaObjectId);
        groupWriter.flushToFileWriter(deltaFileWriter);
        deltaFileWriter.endRowGroup(deltaFileWriter.getPos() - memSize);
      }
      long actualTotalRowGroupSize = deltaFileWriter.getPos() - totalMemStart;
      if (isFillRowGroup) {
        fillInRowGroupSize(actualTotalRowGroupSize);
        LOG.info("total row group size:{}, actual:{}, filled:{}", primaryRowGroupSize,
            actualTotalRowGroupSize, primaryRowGroupSize - actualTotalRowGroupSize);
      } else
        LOG.info("total row group size:{}, row group is not filled", actualTotalRowGroupSize);
      LOG.info("write row group end");
      recordCount = 0;
      reset();
    }
  }

  protected void fillInRowGroupSize(long actualRowGroupSize) throws IOException {
    if (actualRowGroupSize > primaryRowGroupSize)
      LOG.warn("too large actual row group size!:actual:{},threshold:{}", actualRowGroupSize,
          primaryRowGroupSize);
    deltaFileWriter.fillInRowGroup(primaryRowGroupSize - actualRowGroupSize);
  }

  /**
   * <b>Note that</b> we don't need to reset RowGroupWriter explicitly, since after calling
   * {@code flushToFileWriter()}, RowGroupWriter resets itself.
   */
  private void reset() {
    schema.resetUnusedDeltaObjectId(groupWriters);
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

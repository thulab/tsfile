package cn.edu.tsinghua.tsfile.timeseries.basis;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import cn.edu.tsinghua.tsfile.timeseries.read.query.OnePassQueryDataSet;
import org.json.JSONObject;

import cn.edu.tsinghua.tsfile.common.conf.TSFileConfig;
import cn.edu.tsinghua.tsfile.common.conf.TSFileDescriptor;
import cn.edu.tsinghua.tsfile.common.constant.JsonFormatConstant;
import cn.edu.tsinghua.tsfile.common.utils.ITsRandomAccessFileWriter;
import cn.edu.tsinghua.tsfile.common.utils.ITsRandomAccessFileReader;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.FilterExpression;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.FilterFactory;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.SingleSeriesFilterExpression;
import cn.edu.tsinghua.tsfile.timeseries.read.management.SeriesSchema;
import cn.edu.tsinghua.tsfile.timeseries.read.support.Path;
import cn.edu.tsinghua.tsfile.timeseries.read.query.QueryEngine;
import cn.edu.tsinghua.tsfile.timeseries.utils.RecordUtils;
import cn.edu.tsinghua.tsfile.timeseries.write.TsFileWriter;
import cn.edu.tsinghua.tsfile.timeseries.write.exception.WriteProcessException;
import cn.edu.tsinghua.tsfile.timeseries.write.record.TSRecord;
import cn.edu.tsinghua.tsfile.timeseries.write.schema.FileSchema;

/**
 * @author Jinrui Zhang
 */
public class TsFile {

  /** status of current TsFile, 0 means write, 1 means read **/
  private static final int WRITE = 0;
  private static final int READ = 1;

  /** the query engine for this TsFile **/
  private QueryEngine queryEngine;
  /** current status (write or read) **/
  private int status;
  /** writer of this TsFile **/
  private TsFileWriter writer;
  /** schema of thie TsFile **/
  private FileSchema fileSchema;

  /**
   * init with File, schema and default configuration
   * For Write
   *
   * @param file
   *          a TsFile
   * @param schemaJson
   *          the fileSchema of TsFile in type of JSON
   * @throws IOException
   *           exception in IO
   * @throws WriteProcessException
   *           exception in write process
   */
  public TsFile(File file, JSONObject schemaJson) throws IOException, WriteProcessException {
    this(file, new FileSchema(schemaJson));
  }

  /**
   * init with File, schema and default configuration
   * For Write
   *
   * @param file
   *          a TsFile
   * @param schema
   *          the fileSchema of TsFile
   * @throws IOException
   *           cannot write TsFile
   * @throws WriteProcessException
   *           error occurs when writing
   */
  public TsFile(File file, FileSchema schema) throws IOException, WriteProcessException {
    this(schema);
    writer = new TsFileWriter(file, fileSchema, TSFileDescriptor.getInstance().getConfig());
  }

  /**
   * init with File, schema and default configuration
   * For Write
   *
   * @param output
   *          a TsFile
   * @param schemaJson
   *          the fileSchema of TsFile in type of JSON
   * @throws IOException
   *           exception in IO
   * @throws WriteProcessException
   *           exception in write process
   */
  public TsFile(ITsRandomAccessFileWriter output, JSONObject schemaJson)
      throws IOException, WriteProcessException {
    this(new FileSchema(schemaJson));
    TSFileConfig conf = TSFileDescriptor.getInstance().getConfig();
    writer = new TsFileWriter(output, fileSchema, conf);
  }

  /**
   * init with File, schema and default configuration
   * For Write
   *
   * @param output
   *          a TsFile
   * @param schema
   *          the fileSchema of TsFile
   * @throws IOException
   *           cannot write TsFile
   * @throws WriteProcessException
   *           error occurs when writing
   */
  public TsFile(ITsRandomAccessFileWriter output, FileSchema schema)
      throws IOException, WriteProcessException {
    this(schema);
    writer = new TsFileWriter(output, fileSchema, TSFileDescriptor.getInstance().getConfig());
  }

  /**
   * init with schema and default configuration
   * for write
   * @param schema
   */
  private TsFile(FileSchema schema) {
    fileSchema = schema;
    TSFileConfig conf = TSFileDescriptor.getInstance().getConfig();
    if (fileSchema.hasProp(JsonFormatConstant.ROW_GROUP_SIZE))
      conf.groupSizeInByte = Integer.valueOf(fileSchema.getProp(JsonFormatConstant.ROW_GROUP_SIZE));
    if (fileSchema.hasProp(JsonFormatConstant.PAGE_SIZE))
      conf.pageSizeInByte = Integer.valueOf(fileSchema.getProp(JsonFormatConstant.PAGE_SIZE));
    // set status to WRITE
    this.status = WRITE;
  }

  /**
   * init with file reader
   * Notice: This constructor is only for reading TsFile.
   *
   * @param raf
   *          input reader
   * @throws IOException
   *           cannot read TsFile
   */
  public TsFile(ITsRandomAccessFileReader raf) throws IOException {
    this.status = READ;
    queryEngine = new QueryEngine(raf);
    // recordReader = queryEngine.recordReader;
  }

  /**
   * write a line into TsFile <br>
   * the corresponding schema must be defined.
   * 
   * @param line
   *          a line of data
   * @throws IOException
   *           thrown if write process meats IOException like the output stream is closed
   *           abnormally.
   * @throws WriteProcessException
   *           thrown if given data is not matched to fileSchema
   */
  public void writeLine(String line) throws IOException, WriteProcessException {
    // check status
    checkStatus(WRITE);
    // parse String {@code line} to TSRecord and write
    TSRecord record = RecordUtils.parseSimpleTupleRecord(line, fileSchema);
    writer.write(record);
  }

  /**
   * add a new property to this.schema, replace old value if already exist.
   *
   * @param key
   *          key of property
   * @param value
   *          value of property
   */
  public void addProp(String key, String value) {
    fileSchema.addProp(key, value);
  }

  /**
   * write a TSRecord into TsFile.
   *
   * @param tsRecord
   *          a line of data in form of {@linkplain TSRecord}
   * @throws IOException
   *           thrown if write process meats IOException like the output stream is closed
   *           abnormally.
   * @throws WriteProcessException
   *           thrown if given data is not matched to fileSchema
   */
  public void writeRecord(TSRecord tsRecord) throws IOException, WriteProcessException {
    checkStatus(WRITE);
    writer.write(tsRecord);
  }

  /**
   * end the write process normally.
   *
   * @throws IOException
   *           thrown if write process meats IOException like the output stream is closed
   *           abnormally.
   */
  public void close() throws IOException {
    if (this.status == WRITE) {
      // if status is WRITE, close this.writer
      writer.close();
    } else if (this.status == READ) {
      // else, close this.queryEngine
      queryEngine.close();
    } else {
      String[] msg = new String[] { "WRITE", "READ" };
      throw new IOException("This method should be invoked in status " + msg[status]
          + ", but current status is " + msg[this.status]);
    }
  }


  /**
   * read data with time and value filters from given paths
   * @param paths
   * @param timeFilter
   * @param valueFilter
   * @return
   * @throws IOException
   */
  public OnePassQueryDataSet query(List<Path> paths, FilterExpression timeFilter,
                                   FilterExpression valueFilter) throws IOException {
    // check status
    checkStatus(READ);
    // check if {@code valueFilter} is single series filter
    if (paths.size() == 1 && valueFilter instanceof SingleSeriesFilterExpression
        && paths.get(0).getDeltaObjectToString()
            .equals(valueFilter.getFilterSeries().getDeltaObjectUID())
        && paths.get(0).getMeasurementToString()
            .equals(valueFilter.getFilterSeries().getMeasurementUID())) {

    } else if (valueFilter != null) {
      // if not, use corss series And operator to combine
      valueFilter = FilterFactory.csAnd(valueFilter, valueFilter);
    }
    // read data through this.queryEngine
    return queryEngine.query(paths, timeFilter, null, valueFilter);
  }

  /**
   * read data with params, time filter and value filter from given paths
   * @param paths
   * @param timeFilter
   * @param valueFilter
   * @param params
   * @return
   * @throws IOException
   */
  public OnePassQueryDataSet query(List<Path> paths, FilterExpression timeFilter,
                                   FilterExpression valueFilter, Map<String, Long> params) throws IOException {
    // check status
    checkStatus(READ);
    // read data through this.queryEngine
    return queryEngine.query(paths, timeFilter, null, valueFilter, params);
  }

  /**
   * Get All information of column(s) for every deltaObject.
   *
   * @return A set of ArrayList SeriesSchema stored in a HashMap separated by deltaObjectId
   * @throws IOException
   *           thrown if fail to get all series schema
   */
  public Map<String, ArrayList<SeriesSchema>> getAllColumns() throws IOException {
    checkStatus(READ);
    return queryEngine.getAllSeriesSchemasGroupByDeltaObject();
  }

  /**
   * Get RowGroupSize for every deltaObject
   *
   * @return HashMap
   * @throws IOException
   *           thrown if fail to get row group count
   */
  public Map<String, Integer> getDeltaObjectRowGroupCount() throws IOException {
    checkStatus(READ);
    return queryEngine.getDeltaObjectRowGroupCount();
  }

  /**
   * Get Type for every deltaObject
   *
   * @return a map contains all DeltaObjects with type each.
   * @throws IOException
   *           thrown if fail to get delta object type
   */
  public Map<String, String> getDeltaObjectTypes() throws IOException {
    checkStatus(READ);
    return queryEngine.getDeltaObjectTypes();
  }

  /**
   * Check whether given path exists in this TsFile.
   *
   * @param path
   *          A path of one Series
   * @return if the path exists
   * @throws IOException
   *           thrown if fail to check path exists
   */
  public boolean pathExist(Path path) throws IOException {
    checkStatus(READ);
    return queryEngine.pathExist(path);
  }

  /**
   * get all delta obejct ids of this TsFile
   *
   * @return all deltaObjects' name in current TsFile
   * @throws IOException
   *           thrown if fail to get all delta object
   */
  public ArrayList<String> getAllDeltaObject() throws IOException {
    checkStatus(READ);
    return queryEngine.getAllDeltaObject();
  }

  /**
   * get all series schemas of this TsFile
   *
   * @return all series' schemas in current TsFile
   * @throws IOException
   *           thrown if fail to all series
   */
  public List<SeriesSchema> getAllSeries() throws IOException {
    checkStatus(READ);
    return queryEngine.getAllSeriesSchema();
  }

  /**
   * Get all RowGroups' offsets in current TsFile
   *
   * @return res.get(i) represents the End-Position for specific rowGroup i in this file.
   * @throws IOException
   *           thrown if fail to get row group pos list
   */
  public ArrayList<Long> getRowGroupPosList() throws IOException {
    checkStatus(READ);
    return queryEngine.getRowGroupPosList();
  }

  /**
   * get all indexes of RowGroups whose file offset is between {@code start} and {@code end}
   * @param start
   * @param end
   * @return
   * @throws IOException
   */
  public ArrayList<Integer> calSpecificRowGroupByPartition(long start, long end)
      throws IOException {
    checkStatus(READ);
    return queryEngine.calSpecificRowGroupByPartition(start, end);
  }

  /**
   * get all deltaObjectIds of RowGroups whose file offset is between {@code start} and {@code end}
   * @param start
   * @param end
   * @return
   * @throws IOException
   */
  public ArrayList<String> getAllDeltaObjectUIDByPartition(long start, long end)
      throws IOException {
    checkStatus(READ);
    return queryEngine.getAllDeltaObjectUIDByPartition(start, end);
  }

  /**
   * get properties of this.queryEngine
   * @return
   */
  public Map<String, String> getProps() {
    return queryEngine.getProps();
  }

  /**
   * clear and set new properties.
   *
   * @param props
   *          properties in map struct
   */
  public void setProps(Map<String, String> props) {
    fileSchema.setProps(props);
  }

  /**
   * get one specific property
   * @param key
   * @return
   */
  public String getProp(String key) {
    return queryEngine.getProp(key);
  }

  /**
   * make sure input {@code status} equals current {@code status}
   * @param status
   * @throws IOException
   */
  private void checkStatus(int status) throws IOException {
    if (status != this.status) {
      String[] msg = new String[] { "WRITE", "READ" };
      throw new IOException("This method should be invoked in status " + msg[status]
          + ", but current status is " + msg[this.status]);
    }
  }
}

package cn.edu.thu.tsfile.timeseries.basis;

import cn.edu.thu.tsfile.common.conf.TSFileConfig;
import cn.edu.thu.tsfile.common.conf.TSFileDescriptor;
import cn.edu.thu.tsfile.common.constant.JsonFormatConstant;
import cn.edu.thu.tsfile.common.utils.TSRandomAccessFileReader;
import cn.edu.thu.tsfile.common.utils.TSRandomAccessFileWriter;
import cn.edu.thu.tsfile.timeseries.filter.definition.FilterExpression;
import cn.edu.thu.tsfile.timeseries.filter.definition.FilterFactory;
import cn.edu.thu.tsfile.timeseries.filter.definition.SingleSeriesFilterExpression;
import cn.edu.thu.tsfile.timeseries.read.metadata.SeriesSchema;
import cn.edu.thu.tsfile.timeseries.read.qp.Path;
import cn.edu.thu.tsfile.timeseries.read.query.QueryDataSet;
import cn.edu.thu.tsfile.timeseries.read.query.QueryEngine;
import cn.edu.thu.tsfile.timeseries.utils.RecordUtils;
import cn.edu.thu.tsfile.timeseries.write.InternalRecordWriter;
import cn.edu.thu.tsfile.timeseries.write.TSRecordWriteSupport;
import cn.edu.thu.tsfile.timeseries.write.TSRecordWriter;
import cn.edu.thu.tsfile.timeseries.write.WriteSupport;
import cn.edu.thu.tsfile.timeseries.write.exception.WriteProcessException;
import cn.edu.thu.tsfile.timeseries.write.io.TSFileIOWriter;
import cn.edu.thu.tsfile.timeseries.write.record.TSRecord;
import cn.edu.thu.tsfile.timeseries.write.schema.FileSchema;
import org.json.JSONObject;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author Jinrui Zhang
 */
public class TsFile {

    private static final int WRITE = 0;
    private static final int READ = 1;
    private QueryEngine queryEngine;
    private int status;
    private InternalRecordWriter<TSRecord> innerWriter;
    private FileSchema fileSchema;

    /**
     * For Write
     *
     * @param tsFileOutputStream an output stream of TsFile
     * @param schemaJson         the fileSchema of TsFile in type of JSON
     */
    public TsFile(TSRandomAccessFileWriter tsFileOutputStream, JSONObject schemaJson)
            throws IOException, WriteProcessException {
        this.status = WRITE;
        fileSchema = new FileSchema(schemaJson);
        WriteSupport<TSRecord> writeSupport = new TSRecordWriteSupport();
        TSFileIOWriter tsfileWriter = new TSFileIOWriter(fileSchema, tsFileOutputStream);
        TSFileConfig conf = TSFileDescriptor.getInstance().getConfig();
        if (schemaJson.has(JsonFormatConstant.ROW_GROUP_SIZE))
            conf.groupSizeInByte = schemaJson.getInt(JsonFormatConstant.ROW_GROUP_SIZE);
        if (schemaJson.has(JsonFormatConstant.PAGE_SIZE))
            conf.pageSizeInByte = schemaJson.getInt(JsonFormatConstant.PAGE_SIZE);
        innerWriter = new TSRecordWriter(conf, tsfileWriter, writeSupport, fileSchema);
    }

    /**
     * For Write
     *
     * @param tsFileOutputStream an output stream of TsFile
     * @param schema             the fileSchema of TsFile
     * @throws IOException       cannot write TsFile
     * @throws  WriteProcessException error occurs when writing
     */
    public TsFile(TSRandomAccessFileWriter tsFileOutputStream, FileSchema schema)
            throws IOException, WriteProcessException {
        this.status = WRITE;
        fileSchema = schema;
        WriteSupport<TSRecord> writeSupport = new TSRecordWriteSupport();
        TSFileIOWriter tsfileWriter = new TSFileIOWriter(fileSchema, tsFileOutputStream);
        TSFileConfig conf = TSFileDescriptor.getInstance().getConfig();
        if (fileSchema.hasProp(JsonFormatConstant.ROW_GROUP_SIZE))
            conf.groupSizeInByte = Integer.valueOf(fileSchema.getProp(JsonFormatConstant.ROW_GROUP_SIZE));
        if (fileSchema.hasProp(JsonFormatConstant.PAGE_SIZE))
            conf.pageSizeInByte = Integer.valueOf(fileSchema.getProp(JsonFormatConstant.PAGE_SIZE));
        innerWriter = new TSRecordWriter(conf, tsfileWriter, writeSupport, fileSchema);
    }

    /**
     * Notice: This constructor is only for reading TsFile.
     *
     * @param raf input reader
     * @throws IOException cannot read TsFile
     */
    public TsFile(TSRandomAccessFileReader raf) throws IOException {
        this.status = READ;
        queryEngine = new QueryEngine(raf);
//        recordReader = queryEngine.recordReader;
    }

    /**
     * write a line into TsFile
     *
     * @param line a line of data
     * @throws IOException           thrown if write process meats IOException like the output stream is closed abnormally.
     * @throws WriteProcessException thrown if given data is not matched to fileSchema
     */
    public void writeLine(String line) throws IOException, WriteProcessException {
        checkStatus(WRITE);
        TSRecord record = RecordUtils.parseSimpleTupleRecord(line, fileSchema);
        innerWriter.write(record);
    }

    /**
     * add a new property, replace old value if already exist
     *
     * @param key
     * @param value
     */
    public void addProp(String key, String value) {
        fileSchema.addProp(key, value);
    }

    /**
     * write a TSRecord into TsFile
     *
     * @param tsRecord a line of data in form of {@linkplain TSRecord}
     * @throws IOException           thrown if write process meats IOException like the output stream is closed abnormally.
     * @throws WriteProcessException thrown if given data is not matched to fileSchema
     */
    public void writeLine(TSRecord tsRecord) throws IOException, WriteProcessException {
        checkStatus(WRITE);
        innerWriter.write(tsRecord);
    }

    /**
     * end the write process normally
     *
     * @throws IOException thrown if write process meats IOException like the output stream is closed abnormally.
     */
    public void close() throws IOException {
        checkStatus(WRITE);
        innerWriter.close();
    }

    public QueryDataSet query(List<Path> paths, FilterExpression timeFilter,
                              FilterExpression valueFilter) throws IOException {
        checkStatus(READ);
        if (paths.size() == 1 && valueFilter instanceof SingleSeriesFilterExpression
                && paths.get(0).getDeltaObjectToString().equals(valueFilter.getFilterSeries().getDeltaObjectUID())
                && paths.get(0).getMeasurementToString().equals(valueFilter.getFilterSeries().getMeasurementUID())) {

        } else if (valueFilter != null) {
            valueFilter = FilterFactory.csAnd(valueFilter, valueFilter);
        }
        return queryEngine.query(paths, timeFilter, null, valueFilter);
    }

    public QueryDataSet query(List<Path> paths, FilterExpression timeFilter,
                              FilterExpression valueFilter, Map<String, Long> params) throws IOException {
        checkStatus(READ);
        return queryEngine.query(paths, timeFilter, null, valueFilter, params);
    }

    /**
     * Get All information of column(s) for every deltaObject
     *
     * @return A set of ArrayList<SeriesSchema> stored in a HashMap separated by deltaObjectId
     * @throws IOException thrown if fail to get all series schema
     */
    public HashMap<String, ArrayList<SeriesSchema>> getAllColumns() throws IOException {
        checkStatus(READ);
        return queryEngine.getAllSeriesSchemasGroupByDeltaObject();
    }

    /**
     * Get RowGroupSize for every deltaObject
     *
     * @return HashMap
     * @throws IOException thrown if fail to get row group count
     */
    public HashMap<String, Integer> getDeltaObjectRowGroupCount() throws IOException {
        checkStatus(READ);
        return queryEngine.getDeltaObjectRowGroupCount();
    }

    /**
     * Get all DeltaObjects with type each.
     * @throws IOException thrown if fail to get delta object type
     */
    public HashMap<String, String> getDeltaObjectTypes() throws IOException {
        checkStatus(READ);
        return queryEngine.getDeltaObjectTypes();
    }

    /**
     * Check whether given path exists in this TsFile
     *
     * @param path A path of one Series
     * @throws IOException thrown if fail to check path exists
     */
    public boolean pathExist(Path path) throws IOException {
        checkStatus(READ);
        return queryEngine.pathExist(path);
    }

    /**
     * Get all deltaObjects' name in current TsFile
     * @throws IOException thrown if fail to get all delta object
     */
    public ArrayList<String> getAllDeltaObject() throws IOException {
        checkStatus(READ);
        return queryEngine.getAllDeltaObject();
    }

    /**
     * Get all series' schemas in current TsFile
     * @throws IOException thrown if fail to all series
     */
    public ArrayList<SeriesSchema> getAllSeries() throws IOException {
        checkStatus(READ);
        return queryEngine.getAllSeriesSchema();
    }

    /**
     * Get all RowGroups' offsets in current TsFile
     *
     * @return res.get(i) represents the End-Position for specific rowGroup i in
     * this file.
     * @throws IOException thrown if fail to get row group pos list
     */
    public ArrayList<Long> getRowGroupPosList() throws IOException {
        checkStatus(READ);
        return queryEngine.getRowGroupPosList();
    }

    public ArrayList<Integer> calSpecificRowGroupByPartition(long start, long end) throws IOException {
        checkStatus(READ);
        return queryEngine.calSpecificRowGroupByPartition(start, end);
    }

    public ArrayList<String> getAllDeltaObjectUIDByPartition(long start, long end) throws IOException {
        checkStatus(READ);
        return queryEngine.getAllDeltaObjectUIDByPartition(start, end);
    }

    public Map<String, String> getProps() {
        return queryEngine.getProps();
    }

    /**
     * clear and set new properties
     *
     * @param props properties in map struct
     */
    public void setProps(Map<String, String> props) {
        fileSchema.setProps(props);
    }

    public String getProp(String key) {
        return queryEngine.getProp(key);
    }

    private void checkStatus(int status) throws IOException {
        if (status != this.status) {
            String[] msg = new String[]{"WRITE", "READ"};
            throw new IOException("This method should be invoked in status " + msg[status]
                    + ", but current status is " + msg[this.status]);
        }
    }
}

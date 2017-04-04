package cn.edu.thu.tsfile.timeseries.FileFormat;

import cn.edu.thu.tsfile.common.conf.TSFileConfig;
import cn.edu.thu.tsfile.common.conf.TSFileDescriptor;
import cn.edu.thu.tsfile.common.constant.JsonFormatConstant;
import cn.edu.thu.tsfile.common.utils.TSRandomAccessFileReader;
import cn.edu.thu.tsfile.common.utils.TSRandomAccessFileWriter;
import cn.edu.thu.tsfile.timeseries.filter.definition.FilterExpression;
import cn.edu.thu.tsfile.timeseries.filter.definition.FilterFactory;
import cn.edu.thu.tsfile.timeseries.filter.definition.SingleSeriesFilterExpression;
import cn.edu.thu.tsfile.timeseries.filter.definition.filterseries.FilterSeries;
import cn.edu.thu.tsfile.timeseries.read.RecordReader;
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
    private RecordReader recordReader;
    private int status;

    //add by Kangrong start
    private InternalRecordWriter<TSRecord> innerWriter;
    private FileSchema fileSchema;
    //add by Kangrong end

    /**
     * For Write
     *
     * @param tsFileOutputStream an output stream of TsFile
     * @param schemaJson       the fileSchema of TsFile in type of JSON
     */
    public TsFile(TSRandomAccessFileWriter tsFileOutputStream, JSONObject schemaJson)
            throws IOException, WriteProcessException {
        this.status = WRITE;
        fileSchema = new FileSchema(schemaJson);
        WriteSupport<TSRecord> writeSupport = new TSRecordWriteSupport();
        TSFileIOWriter tsfileWriter = new TSFileIOWriter(fileSchema, tsFileOutputStream);
        TSFileConfig conf = TSFileDescriptor.getInstance().getConfig();
        if (schemaJson.has(JsonFormatConstant.ROW_GROUP_SIZE))
            conf.rowGroupSize = schemaJson.getInt(JsonFormatConstant.ROW_GROUP_SIZE);
        if (schemaJson.has(JsonFormatConstant.PAGE_SIZE))
            conf.pageSize = schemaJson.getInt(JsonFormatConstant.PAGE_SIZE);
        innerWriter = new TSRecordWriter(conf, tsfileWriter, writeSupport, fileSchema);
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

    /**
     * Notice: This constructor is only for reading TsFile.
     *
     * @param raf
     * @throws IOException
     */
    public TsFile(TSRandomAccessFileReader raf) throws IOException {
        this.status = READ;
        queryEngine = new QueryEngine(raf);
        recordReader = queryEngine.recordReader;
    }

    /**
     * Check whether thf file given is a TsFile
     *
     * @param raf
     * @return
     */
    private boolean isTsFile(TSRandomAccessFileReader raf) {
        return true;
    }

    public QueryDataSet query(List<Path> paths, FilterExpression timeFilter,
                              FilterExpression valueFilter) throws IOException {
        checkStatus(READ);
        if (paths.size()==1 && valueFilter instanceof SingleSeriesFilterExpression
                && paths.get(0).getDeltaObjectToString().equals(valueFilter.getFilterSeries().getDeltaObjectUID())
                && paths.get(0).getMeasurementToString().equals(valueFilter.getFilterSeries().getMeasurementUID())) {

        } else if (valueFilter != null){
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
     * @throws IOException
     */
    public HashMap<String, ArrayList<SeriesSchema>> getAllColumns() throws IOException {
        checkStatus(READ);
        return recordReader.getAllSeriesSchemasGroupByDeltaObject();
    }

    /**
     * Get RowGroupSize for every deltaObject
     *
     * @return HashMap
     * @throws IOException
     */
    public HashMap<String, Integer> getDeltaObjectRowGroupCount() throws IOException {
        checkStatus(READ);
        return recordReader.getDeltaObjectRowGroupCounts();
    }

    /**
     * Get all DeltaObjects with type each.
     */
    public HashMap<String, String> getDeltaObjectTypes() throws IOException {
        checkStatus(READ);
        return recordReader.getDeltaObjectTypes();
    }

    /**
     * Check whether given path exists in this TsFile
     * @param path A path of one Series
     * @throws IOException
     */
    public boolean pathExist(Path path) throws IOException {
        checkStatus(READ);
        FilterSeries<?> col = recordReader.getColumnByMeasurementName(path.getDeltaObjectToString(), path.getMeasurementToString());
        return col != null;
    }

    /**
     * Get all deltaObjects' name in current TsFile
     */
    public ArrayList<String> getAllDeltaObject() throws IOException {
        checkStatus(READ);
        return recordReader.getAllDeltaObjects();
    }

    /**
     * Get all series' schemas in current TsFile
     */
    public ArrayList<SeriesSchema> getAllSeries() throws IOException {
        checkStatus(READ);
        return recordReader.getAllSeriesSchema();
    }

    /**
     * Get all RowGroups' offsets in current TsFile
     * @return res.get(i) represents the End-Position for specific rowGroup i in
     *         this file.
     */
    public ArrayList<Long> getRowGroupPosList() throws IOException {
        checkStatus(READ);
        return recordReader.getRowGroupPosList();
    }

    public ArrayList<Integer> calSpecificRowGroupByPartition(long start, long end) throws IOException {
        checkStatus(READ);
        return queryEngine.calSpecificRowGroupByPartition(start, end);
    }

    public ArrayList<String> getAllDeltaObjectUIDByPartition(long start, long end) throws IOException {
        checkStatus(READ);
        return queryEngine.getAllDeltaObjectUIDByPartition(start, end);
    }

    private void checkStatus(int status) throws IOException {
        if(status != this.status){
            String[] msg = new String[]{"WRITE", "READ"};
            throw new IOException("This method should be invoked in status " + msg[status]
                    + ", but current status is " + msg[this.status]);
        }
    }
}

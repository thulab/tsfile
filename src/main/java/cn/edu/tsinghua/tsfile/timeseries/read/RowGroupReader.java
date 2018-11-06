package cn.edu.tsinghua.tsfile.timeseries.read;

import cn.edu.tsinghua.tsfile.common.utils.ITsRandomAccessFileReader;
import cn.edu.tsinghua.tsfile.file.metadata.RowGroupMetaData;
import cn.edu.tsinghua.tsfile.file.metadata.TimeSeriesChunkMetaData;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.SingleSeriesFilterExpression;
import cn.edu.tsinghua.tsfile.timeseries.read.query.DynamicOneColumnData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author Jinrui Zhang
 * This class is used to read one RowGroup.
 */
public class RowGroupReader {

    protected static final Logger logger = LoggerFactory.getLogger(RowGroupReader.class);

    /** map of series name and TSDataType **/
    public Map<String, TSDataType> seriesDataTypeMap;
    /** Map<measurementId, ValueReader> of all ValueReaders **/
    protected Map<String, ValueReader> valueReaders = new HashMap<>();
    /** deltaObjectUID of this RowGroup **/
    protected String deltaObjectUID;

    /** list of measurementIds fo this RowGroup **/
    protected List<String> measurementIds;
    /** total bytes size of this RowGroupMetaData **/
    protected long totalByteSize;

    /** tsfile reader **/
    protected ITsRandomAccessFileReader raf;

    /**
     * init nothing
     */
    public RowGroupReader() {

    }

    /**
     * init by RowGroupMetaData and tsfile reader
     * @param rowGroupMetaData
     * @param raf
     */
    public RowGroupReader(RowGroupMetaData rowGroupMetaData, ITsRandomAccessFileReader raf) {
        logger.debug(String.format("init a new RowGroupReader, the deltaObjectId is %s", rowGroupMetaData.getDeltaObjectID()));
        seriesDataTypeMap = new HashMap<>();
        deltaObjectUID = rowGroupMetaData.getDeltaObjectID();
        measurementIds = new ArrayList<>();
        this.totalByteSize = rowGroupMetaData.getTotalByteSize();
        this.raf = raf;

        // init ValueReaders by {@code rowGroupMetaData}
        initValueReaders(rowGroupMetaData);
    }

    /**
     * get all Objects whose index is in {@code retMap}
     * @param timeRet
     * @param retMap
     * @return
     */
    public List<Object> getTimeByRet(List<Object> timeRet, HashMap<Integer, Object> retMap) {
        List<Object> timeRes = new ArrayList<Object>();
        for (Integer i : retMap.keySet()) {
            timeRes.add(timeRet.get(i));
        }
        return timeRes;
    }

    /**
     * get TSDataType from {@code seriesDataTypeMap} by name
     * @param name
     * @return
     */
    public TSDataType getDataTypeBySeriesName(String name) {
        return this.seriesDataTypeMap.get(name);
    }

    public String getDeltaObjectUID() {
        return this.deltaObjectUID;
    }

    /**
     * Read time-value pairs whose time is be included in timeRet. WARNING: this
     * function is only for "time" Series
     *
     * @param measurementId measurement's id
     * @param timeRet       Array of the time.
     * @return DynamicOneColumnData
     * @throws IOException exception in IO
     */
    public DynamicOneColumnData readValueUseTimestamps(String measurementId, long[] timeRet) throws IOException {
        logger.debug("query {}.{} using common time, time length : {}", deltaObjectUID, measurementId, timeRet.length);
        return valueReaders.get(measurementId).getValuesForGivenValues(timeRet);
    }

    /**
     * read value from corresponding ValueReader in {@code valueReaders}
     * @param sid measurementId
     * @param res result
     * @param fetchSize bytes size to fetch
     * @param timeFilter
     * @param freqFilter
     * @param valueFilter
     * @return
     * @throws IOException
     */
    public DynamicOneColumnData readOneColumnUseFilter(String sid, DynamicOneColumnData res, int fetchSize
            , SingleSeriesFilterExpression timeFilter, SingleSeriesFilterExpression freqFilter, SingleSeriesFilterExpression valueFilter) throws IOException {
        ValueReader valueReader = valueReaders.get(sid);
        return valueReader.readOneColumnUseFilter(res, fetchSize, timeFilter, freqFilter, valueFilter);
    }

    /**
     * read value from corresponding ValueReader in {@code valueReaders}
     * @param sid measurementId
     * @param res result
     * @param fetchSize bytes size to fetch
     * @return
     * @throws IOException
     */
    public DynamicOneColumnData readOneColumn(String sid, DynamicOneColumnData res, int fetchSize) throws IOException {
        ValueReader valueReader = valueReaders.get(sid);
        return valueReader.readOneColumn(res, fetchSize);
    }

    /**
     * get corresponding ValueReader of input {@code sid}
     * @param sid measurementId
     * @return
     */
    public ValueReader getValueReaderForSpecificMeasurement(String sid) {
        return getValueReaders().get(sid);
    }

    public long getTotalByteSize() {
        return totalByteSize;
    }

    public void setTotalByteSize(long totalByteSize) {
        this.totalByteSize = totalByteSize;
    }

    public Map<String, ValueReader> getValueReaders() {
        return valueReaders;
    }

    public void setValueReaders(HashMap<String, ValueReader> valueReaders) {
        this.valueReaders = valueReaders;
    }

    public ITsRandomAccessFileReader getRaf() {
        return raf;
    }

    public void setRaf(ITsRandomAccessFileReader raf) {
        this.raf = raf;
    }

    public boolean containsMeasurement(String measurementID) {
        return this.valueReaders.containsKey(measurementID);
    }

    public void close() throws IOException {
        this.raf.close();
    }

    /**
     * init {@code measurementIds}, {@code seriesDataTypeMap} and {@code valueReaders} by input RowGroupMetaData
     * @param rowGroupMetaData input RowGroupMetaData
     */
    public void initValueReaders(RowGroupMetaData rowGroupMetaData) {
        // loop all TimeSeriesChunkMetaData in input RowGroupMetaData
        for (TimeSeriesChunkMetaData tscMetaData : rowGroupMetaData.getTimeSeriesChunkMetaDataList()) {
            if (tscMetaData.getVInTimeSeriesChunkMetaData() != null) {
                // add measurementId
                measurementIds.add(tscMetaData.getProperties().getMeasurementUID());

                // add <measurementId, TSDataType>
                seriesDataTypeMap.put(tscMetaData.getProperties().getMeasurementUID(),
                        tscMetaData.getVInTimeSeriesChunkMetaData().getDataType());

                // init ValueReader by current TimeSeriesChunkMetaData
                ValueReader si = new ValueReader(tscMetaData.getProperties().getFileOffset(),
                        tscMetaData.getTotalByteSize(),
                        tscMetaData.getVInTimeSeriesChunkMetaData().getDataType(),
                        tscMetaData.getVInTimeSeriesChunkMetaData().getDigest(), this.raf,
                        tscMetaData.getVInTimeSeriesChunkMetaData().getEnumValues(),
                        tscMetaData.getProperties().getCompression(), tscMetaData.getNumRows(),
                        tscMetaData.getTInTimeSeriesChunkMetaData().getStartTime(), tscMetaData.getTInTimeSeriesChunkMetaData().getEndTime());
                // add <measurementId, ValueReader>
                valueReaders.put(tscMetaData.getProperties().getMeasurementUID(), si);
            }
        }
    }
}
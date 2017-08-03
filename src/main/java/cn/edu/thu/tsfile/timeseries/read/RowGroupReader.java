package cn.edu.thu.tsfile.timeseries.read;

import cn.edu.thu.tsfile.common.utils.TSRandomAccessFileReader;
import cn.edu.thu.tsfile.file.metadata.RowGroupMetaData;
import cn.edu.thu.tsfile.file.metadata.TimeSeriesChunkMetaData;
import cn.edu.thu.tsfile.file.metadata.enums.TSDataType;
import cn.edu.thu.tsfile.timeseries.filter.definition.SingleSeriesFilterExpression;
import cn.edu.thu.tsfile.timeseries.read.query.DynamicOneColumnData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * @author Jinrui Zhang
 * This class is used to read one RowGroup.
 */
public class RowGroupReader {

    protected static final Logger logger = LoggerFactory.getLogger(RowGroupReader.class);
    public HashMap<String, TSDataType> seriesDataTypeMap;
    protected HashMap<String, ValueReader> valueReaders = new HashMap<>();
    protected String deltaObjectUID;

    protected ArrayList<String> sids;
    protected String deltaObjectType;
    protected long totalByteSize;

    protected TSRandomAccessFileReader raf;

    public RowGroupReader(RowGroupMetaData rowGroupMetaData, TSRandomAccessFileReader raf) {
        logger.debug("init a new RowGroupReader..");
        seriesDataTypeMap = new HashMap<>();
        deltaObjectUID = rowGroupMetaData.getDeltaObjectUID();
        sids = new ArrayList<>();
        deltaObjectType = rowGroupMetaData.getDeltaObjectType();
        this.totalByteSize = rowGroupMetaData.getTotalByteSize();
        this.raf = raf;

        for (TimeSeriesChunkMetaData tscMetaData : rowGroupMetaData.getTimeSeriesChunkMetaDataList()) {
            if (tscMetaData.getVInTimeSeriesChunkMetaData() != null) {
                sids.add(tscMetaData.getProperties().getMeasurementUID());
                seriesDataTypeMap.put(tscMetaData.getProperties().getMeasurementUID(),
                        tscMetaData.getVInTimeSeriesChunkMetaData().getDataType());

                ValueReader si = new ValueReader(tscMetaData.getProperties().getFileOffset(),
                        tscMetaData.getTotalByteSize(),
                        tscMetaData.getVInTimeSeriesChunkMetaData().getDataType(),
                        tscMetaData.getVInTimeSeriesChunkMetaData().getDigest(), this.raf,
                        tscMetaData.getVInTimeSeriesChunkMetaData().getEnumValues(),
                        tscMetaData.getProperties().getCompression(), tscMetaData.getNumRows());
                valueReaders.put(tscMetaData.getProperties().getMeasurementUID(), si);
            }
        }
    }

    public List<Object> getTimeByRet(List<Object> timeRet, HashMap<Integer, Object> retMap) {
        List<Object> timeRes = new ArrayList<Object>();
        for (Integer i : retMap.keySet()) {
            timeRes.add(timeRet.get(i));
        }
        return timeRes;
    }

    public String getDeltaObjectType() {
        return this.deltaObjectType;
    }

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
     * @throws IOException
     */
    public DynamicOneColumnData readValueUseTimeValue(String measurementId, long[] timeRet) throws IOException {
        return valueReaders.get(measurementId).getValuesForGivenValues(timeRet);
    }

    public DynamicOneColumnData readOneColumnUseFilter(String sid, DynamicOneColumnData res, int fetchSize
            , SingleSeriesFilterExpression timeFilter, SingleSeriesFilterExpression freqFilter, SingleSeriesFilterExpression valueFilter) throws IOException {
        ValueReader valueReader = valueReaders.get(sid);
        return valueReader.readOneColumnUseFilter(res, fetchSize, timeFilter, freqFilter, valueFilter);
    }

    public DynamicOneColumnData readOneColumn(String sid, DynamicOneColumnData res, int fetchSize) throws IOException {
        ValueReader valueReader = valueReaders.get(sid);
        return valueReader.readOneColumn(res, fetchSize);
    }

    public ValueReader getValueReaderForSpecificMeasurement(String sid) {
        return getValueReaders().get(sid);
    }

    public long getTotalByteSize() {
        return totalByteSize;
    }

    public void setTotalByteSize(long totalByteSize) {
        this.totalByteSize = totalByteSize;
    }

    public HashMap<String, ValueReader> getValueReaders() {
        return valueReaders;
    }

    public void setValueReaders(HashMap<String, ValueReader> valueReaders) {
        this.valueReaders = valueReaders;
    }

    public TSRandomAccessFileReader getRaf() {
        return raf;
    }

    public void setRaf(TSRandomAccessFileReader raf) {
        this.raf = raf;
    }
}
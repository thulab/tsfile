package cn.edu.tsinghua.tsfile.timeseries.read;

import cn.edu.tsinghua.tsfile.common.exception.UnSupportedDataTypeException;
import cn.edu.tsinghua.tsfile.common.utils.ITsRandomAccessFileReader;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.FilterFactory;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.SingleSeriesFilterExpression;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.filterseries.FilterSeries;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.filterseries.FilterSeriesType;
import cn.edu.tsinghua.tsfile.timeseries.read.metadata.SeriesSchema;
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
 * This class implements several read methods which can read data in different ways.<br>
 * This class provides some APIs for reading.
 */
public class RecordReader {

    private static final Logger logger = LoggerFactory.getLogger(RecordReader.class);
    private ReaderManager readerManager;
    private HashMap<String, HashMap<String, SeriesSchema>> seriesSchemaMap;

    public RecordReader(String filePath) throws IOException {
        this.readerManager = new ReaderManager(new TsRandomAccessLocalFileReader(filePath));
    }

    public RecordReader(ITsRandomAccessFileReader raf) throws IOException {
        this.readerManager = new ReaderManager(raf);
    }

    /**
     * Read function 1#1: read one column without filter
     *
     * @param res result
     * @param fetchSize fetch size
     * @param deltaObjectUID delta object id
     * @param measurementId  measurement Id
     * @return DynamicOneColumnData
     * @throws IOException failed to get value
     */
    public DynamicOneColumnData getValueInOneColumn(DynamicOneColumnData res, int fetchSize
            , String deltaObjectUID, String measurementId) throws IOException {
        checkSeries(deltaObjectUID, measurementId);
        List<RowGroupReader> rowGroupReaderList = readerManager.getRowGroupReaderListByDeltaObject(deltaObjectUID);
        int i = 0;
        if (res != null) {
            i = res.getRowGroupIndex();
        }
        for (; i < rowGroupReaderList.size(); i++) {
            RowGroupReader rowGroupReader = rowGroupReaderList.get(i);
            res = getValueInOneColumn(res, fetchSize, rowGroupReader, measurementId);
            if (res.valueLength >= fetchSize) {
                res.hasReadAll = false;
                break;
            }
        }
        return res;
    }

    private DynamicOneColumnData getValueInOneColumn(DynamicOneColumnData res, int fetchSize,
                                                     RowGroupReader rowGroupReader, String measurementId) throws IOException {
        return rowGroupReader.getValueReaders().get(measurementId).readOneColumn(res, fetchSize);
    }

    /**
     * Read function 1#3: read one column without filter from one specific
     * RowGroupReader according to the index
     * @param res result
     * @param fetchSize fetch size
     * @param deltaObjectUID delta object id
     * @param measurementId  measurement Id
     * @param idx index of the RowGroupReader
     * @return DynamicOneColumnData
     * @throws IOException failed to get value
     */
    public DynamicOneColumnData getValueInOneColumn(DynamicOneColumnData res, int fetchSize, String deltaObjectUID,
                                                    String measurementId, int idx) throws IOException {
        checkSeries(deltaObjectUID, measurementId);
        List<RowGroupReader> rowGroupReaderList = readerManager.getRowGroupReaderListByDeltaObject(deltaObjectUID);

        if (idx >= rowGroupReaderList.size()) {
            logger.error("RowGroup index is not right. Index :" + idx + ". Size: " + rowGroupReaderList.size());
            return null;
        }

        RowGroupReader rowGroupReader = rowGroupReaderList.get(idx);
        res = getValueInOneColumn(res, fetchSize, rowGroupReader, measurementId);

        return res;
    }

    /**
     * Read function 1#4: read one column without filter from one specific
     * RowGroupReader(s) according to the indexList
     * @param res result
     * @param fetchSize fetch size
     * @param deltaObjectUID delta object id
     * @param measurementId  measurement Id
     * @param idxes index list of the RowGroupReader
     * @return DynamicOneColumnData
     * @throws IOException failed to get value
     */
    public DynamicOneColumnData getValueInOneColumn(DynamicOneColumnData res, int fetchSize, String deltaObjectUID,
                                                    String measurementId, ArrayList<Integer> idxes) throws IOException {
        checkSeries(deltaObjectUID, measurementId);
        int rowGroupSkipCount = 0;

        List<RowGroupReader> rowGroupReaderList = readerManager.getAllRowGroupReaders();
        int i = 0;
        if (res != null) {
            i = res.getRowGroupIndex();
        }
        for (; i < idxes.size(); i++) {
            int idx = idxes.get(i);
            RowGroupReader rowGroupReader = rowGroupReaderList.get(idx);
            if (!deltaObjectUID.equals(rowGroupReader.getDeltaObjectUID())) {
                rowGroupSkipCount++;
                continue;
            }
            res = getValueInOneColumn(res, fetchSize, rowGroupReader, measurementId);
            for (int k = 0; k < rowGroupSkipCount; k++) {
                res.plusRowGroupIndexAndInitPageOffset();
            }
            if (res.valueLength >= fetchSize) {
                res.hasReadAll = false;
                break;
            }
        }
        return res;
    }

    public DynamicOneColumnData getValuesUseFilter(DynamicOneColumnData res, int fetchSize, String deltaObjectUID,
                                                   String measurementId, SingleSeriesFilterExpression timeFilter, SingleSeriesFilterExpression freqFilter,
                                                   SingleSeriesFilterExpression valueFilter) throws IOException {
        checkSeries(deltaObjectUID, measurementId);
        int i = 0;
        if (res != null) {
            i = res.getRowGroupIndex();
        }

        List<RowGroupReader> rowGroupReaderList = readerManager.getRowGroupReaderListByDeltaObject(deltaObjectUID);
        for (; i < rowGroupReaderList.size(); i++) {
            RowGroupReader rowGroupReader = rowGroupReaderList.get(i);
            res = getValuesUseFilter(res, fetchSize, rowGroupReader, measurementId, timeFilter, freqFilter, valueFilter);
            if (res.valueLength >= fetchSize) {
                res.hasReadAll = false;
                break;
            }
        }

        return res;
    }

    public DynamicOneColumnData getValuesUseFilter(DynamicOneColumnData res, int fetchSize
            , SingleSeriesFilterExpression valueFilter) throws IOException {
        String deltaObjectUID = valueFilter.getFilterSeries().getDeltaObjectUID();
        String measurementUID = valueFilter.getFilterSeries().getMeasurementUID();
        return getValuesUseFilter(res, fetchSize, deltaObjectUID, measurementUID, null, null, valueFilter);
    }

    public DynamicOneColumnData getValuesUseFilter(DynamicOneColumnData res, int fetchSize
            , SingleSeriesFilterExpression valueFilter, ArrayList<Integer> idxs) throws IOException {
        String deltaObjectUID = valueFilter.getFilterSeries().getDeltaObjectUID();
        String measurementUID = valueFilter.getFilterSeries().getMeasurementUID();
        return getValuesUseFilter(res, fetchSize, deltaObjectUID, measurementUID, null, null, valueFilter, idxs);
    }

    private DynamicOneColumnData getValuesUseFilter(DynamicOneColumnData res, int fetchSize,
                                                    RowGroupReader rowGroupReader, String measurementId, SingleSeriesFilterExpression timeFilter,
                                                    SingleSeriesFilterExpression freqFilter, SingleSeriesFilterExpression valueFilter) throws IOException {

        res = rowGroupReader.getValueReaders().get(measurementId).readOneColumnUseFilter(res, fetchSize, timeFilter,
                freqFilter, valueFilter);
        return res;
    }

    public DynamicOneColumnData getValuesUseFilter(DynamicOneColumnData res, int fetchSize, String deltaObjectUID,
                                                   String measurementId, SingleSeriesFilterExpression timeFilter, SingleSeriesFilterExpression freqFilter,
                                                   SingleSeriesFilterExpression valueFilter, int idx) throws IOException {
        checkSeries(deltaObjectUID, measurementId);
        List<RowGroupReader> rowGroupReaderList = readerManager.getRowGroupReaderListByDeltaObject(deltaObjectUID);
        if (idx >= rowGroupReaderList.size()) {
            logger.error("RowGroup index is not right. Index :" + idx + ". Size: " + rowGroupReaderList.size());
            return null;
        }

        RowGroupReader rowGroupReader = rowGroupReaderList.get(idx);
        res = getValuesUseFilter(res, fetchSize, rowGroupReader, measurementId, timeFilter, freqFilter, valueFilter);
        return res;
    }

    public DynamicOneColumnData getValuesUseFilter(DynamicOneColumnData res, int fetchSize, String deltaObjectUID,
                                                   String measurementId, SingleSeriesFilterExpression timeFilter, SingleSeriesFilterExpression freqFilter,
                                                   SingleSeriesFilterExpression valueFilter, ArrayList<Integer> idxs) throws IOException {
        checkSeries(deltaObjectUID, measurementId);
        int rowGroupSkipCount = 0;

        List<RowGroupReader> rowGroupReaderList = readerManager.getAllRowGroupReaders();
        int i = 0;
        if (res != null) {
            i = res.getRowGroupIndex();
        }
        for (; i < idxs.size(); i++) {
            logger.info("GetValuesUseFilter and idxs. RowGroupIndex is :" + idxs.get(i));
            int idx = idxs.get(i);
            RowGroupReader rowGroupReader = rowGroupReaderList.get(idx);
            if (!deltaObjectUID.equals(rowGroupReader.getDeltaObjectUID())) {
                rowGroupSkipCount++;
                continue;
            }
            res = getValuesUseFilter(res, fetchSize, rowGroupReader, measurementId, timeFilter, freqFilter, valueFilter);
            for (int k = 0; k < rowGroupSkipCount; k++) {
                res.plusRowGroupIndexAndInitPageOffset();
            }
            if (res.valueLength >= fetchSize) {
                res.hasReadAll = false;
                break;
            }
        }
        return res;
    }


    public DynamicOneColumnData getValuesUseTimeValue(String deltaObjectUID, String measurementId, long[] timeRet)
            throws IOException {
        checkSeries(deltaObjectUID, measurementId);
        DynamicOneColumnData res = null;
        List<RowGroupReader> rowGroupReaderList = readerManager.getRowGroupReaderListByDeltaObject(deltaObjectUID);
        for (int i = 0; i < rowGroupReaderList.size(); i++) {
            RowGroupReader rowGroupReader = rowGroupReaderList.get(i);
            if (i == 0) {
                res = getValuesUseTimeValue(rowGroupReader, measurementId, timeRet);
            } else {
                DynamicOneColumnData tmpRes = getValuesUseTimeValue(rowGroupReader, measurementId, timeRet);
                res.mergeRecord(tmpRes);
            }
        }
        return res;
    }


    public DynamicOneColumnData getValuesUseTimeValue(String deltaObjectUID, String measurementId, long[] timeRet,
                                                      ArrayList<Integer> idxs) throws IOException {
        checkSeries(deltaObjectUID, measurementId);
        DynamicOneColumnData res = null;
        List<RowGroupReader> rowGroupReaderList = readerManager.getAllRowGroupReaders();

        boolean init = false;
        for (int i = 0; i < idxs.size(); i++) {
            int idx = idxs.get(i);
            RowGroupReader rowGroupReader = rowGroupReaderList.get(idx);
            if (!deltaObjectUID.equals(rowGroupReader.getDeltaObjectUID())) {
                continue;
            }
            if (!init) {
                res = getValuesUseTimeValue(rowGroupReader, measurementId, timeRet);
                init = true;
            } else {
                DynamicOneColumnData tmpRes = getValuesUseTimeValue(rowGroupReader, measurementId, timeRet);
                res.mergeRecord(tmpRes);
            }
        }
        return res;
    }


    private DynamicOneColumnData getValuesUseTimeValue(RowGroupReader rowGroupReader, String measurementId, long[] timeRet)
            throws IOException {
        return rowGroupReader.getValueReaders().get(measurementId).getValuesForGivenValues(timeRet);
    }

    public boolean isEnumsColumn(String deltaObjectUID, String sid) {
        List<RowGroupReader> rowGroupReaderList = readerManager.getRowGroupReaderListByDeltaObject(deltaObjectUID);
        for (RowGroupReader rowGroupReader : rowGroupReaderList) {
            if (rowGroupReader.getValueReaderForSpecificMeasurement(sid) == null) {
                continue;
            }
            if (rowGroupReader.getValueReaders().get(sid).getDataType() == TSDataType.ENUMS) {
                return true;
            }
        }
        return false;
    }

    public ArrayList<SeriesSchema> getAllSeriesSchema() {
        HashMap<String, Integer> seriesMap = new HashMap<>();
        ArrayList<SeriesSchema> res = new ArrayList<>();
        List<RowGroupReader> rowGroupReaders = readerManager.getAllRowGroupReaders();
        for (RowGroupReader rgr : rowGroupReaders) {
            for (String measurement : rgr.seriesDataTypeMap.keySet()) {
                if (!seriesMap.containsKey(measurement)) {
                    res.add(new SeriesSchema(measurement, rgr.seriesDataTypeMap.get(measurement), null));
                    seriesMap.put(measurement, 1);
                }
            }
        }
        return res;
    }

    public ArrayList<String> getAllDeltaObjects() {
        ArrayList<String> res = new ArrayList<>();
        HashMap<String, Integer> deltaObjectMap = new HashMap<>();
        List<RowGroupReader> rowGroupReaders = readerManager.getAllRowGroupReaders();
        for (RowGroupReader rgr : rowGroupReaders) {
            String deltaObjectUID = rgr.getDeltaObjectUID();
            if (!deltaObjectMap.containsKey(deltaObjectUID)) {
                res.add(deltaObjectUID);
                deltaObjectMap.put(deltaObjectUID, 1);
            }
        }
        return res;
    }


    public HashMap<String, ArrayList<SeriesSchema>> getAllSeriesSchemasGroupByDeltaObject() {
        HashMap<String, ArrayList<SeriesSchema>> res = new HashMap<>();
        HashMap<String, List<RowGroupReader>> rowGroupReaders = readerManager.getRowGroupReaderMap();
        for (String deltaObjectUID : rowGroupReaders.keySet()) {
            HashMap<String, Integer> measurementMap = new HashMap<>();
            ArrayList<SeriesSchema> cols = new ArrayList<>();
            for (RowGroupReader rgr : rowGroupReaders.get(deltaObjectUID)) {
                for (String measurement : rgr.seriesDataTypeMap.keySet()) {
                    if (!measurementMap.containsKey(measurement)) {
                        cols.add(new SeriesSchema(measurement, rgr.seriesDataTypeMap.get(measurement), null));
                        measurementMap.put(measurement, 1);
                    }
                }
            }
            res.put(deltaObjectUID, cols);
        }
        return res;
    }

    /**
     * @return all DeltaObjects' name with rowGroup count each.
     */
    public HashMap<String, Integer> getDeltaObjectRowGroupCounts() {
        HashMap<String, Integer> res = new HashMap<>();
        HashMap<String, List<RowGroupReader>> rowGroupReaders = readerManager.getRowGroupReaderMap();
        for (String deltaObjectUID : rowGroupReaders.keySet()) {
            res.put(deltaObjectUID, rowGroupReaders.get(deltaObjectUID).size());
        }
        return res;
    }

    /**
     * @return all DeltaObjects with type each.
     */
    public HashMap<String, String> getDeltaObjectTypes() {
        HashMap<String, String> res = new HashMap<>();
        HashMap<String, List<RowGroupReader>> rowGroupReaders = readerManager.getRowGroupReaderMap();
        for (String deltaObjectUID : rowGroupReaders.keySet()) {

            RowGroupReader rgr = rowGroupReaders.get(deltaObjectUID).get(0);
        }
        return res;
    }


    public ArrayList<Long> getRowGroupPosList() {
        ArrayList<Long> res = new ArrayList<>();
        long startPos = 0;
        for (RowGroupReader rowGroupReader : readerManager.getAllRowGroupReaders()) {
            long currentEndPos = rowGroupReader.getTotalByteSize() + startPos;
            res.add(currentEndPos);
            startPos = currentEndPos;
        }
        return res;
    }


    public FilterSeries<?> getColumnByMeasurementName(String deltaObject, String measurement) {
        TSDataType type = readerManager.getDataTypeBySeriesName(deltaObject, measurement);
        if (type == TSDataType.INT32) {
            return FilterFactory.intFilterSeries(deltaObject, measurement, FilterSeriesType.VALUE_FILTER);
        } else if (type == TSDataType.INT64) {
            return FilterFactory.longFilterSeries(deltaObject, measurement, FilterSeriesType.VALUE_FILTER);
        } else if (type == TSDataType.FLOAT) {
            return FilterFactory.floatFilterSeries(deltaObject, measurement, FilterSeriesType.VALUE_FILTER);
        } else if (type == TSDataType.DOUBLE) {
            return FilterFactory.doubleFilterSeries(deltaObject, measurement, FilterSeriesType.VALUE_FILTER);
        } else if (type == TSDataType.BOOLEAN) {
            return FilterFactory.booleanFilterSeries(deltaObject, measurement, FilterSeriesType.VALUE_FILTER);
        } else if (type == TSDataType.ENUMS || type == TSDataType.TEXT) {
            return FilterFactory.stringFilterSeries(deltaObject, measurement, FilterSeriesType.VALUE_FILTER);
        } else {
            throw new UnSupportedDataTypeException(String.valueOf(type));
        }
    }

    private void checkSeries(String deltaObject, String measurement) throws IOException {
        if (seriesSchemaMap == null) {
            seriesSchemaMap = new HashMap<>();
            HashMap<String, ArrayList<SeriesSchema>> seriesSchemaListMap = getAllSeriesSchemasGroupByDeltaObject();
            for (String key : seriesSchemaListMap.keySet()) {
                HashMap<String, SeriesSchema> tmap = new HashMap<>();
                for (SeriesSchema ss : seriesSchemaListMap.get(key)) {
                    tmap.put(ss.name, ss);
                }
                seriesSchemaMap.put(key, tmap);
            }
        }
        if (seriesSchemaMap.containsKey(deltaObject)) {
            if (seriesSchemaMap.get(deltaObject).containsKey(measurement)) {
                return;
            }
        }
        throw new IOException("Series not exist in current file: " + deltaObject + "#" + measurement);
    }

    public List<RowGroupReader> getAllRowGroupReaders() {
        return readerManager.getAllRowGroupReaders();
    }

    public Map<String, String> getProps() {
        return readerManager.getProps();
    }

    public String getProp(String key) {
        return readerManager.getProp(key);
    }

    public ReaderManager getReaderManager() {
        return readerManager;
    }

    public void close() throws IOException {
        readerManager.close();
    }
}

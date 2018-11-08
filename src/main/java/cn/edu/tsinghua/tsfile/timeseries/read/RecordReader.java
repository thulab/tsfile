package cn.edu.tsinghua.tsfile.timeseries.read;

import cn.edu.tsinghua.tsfile.common.exception.UnSupportedDataTypeException;
import cn.edu.tsinghua.tsfile.common.utils.ITsRandomAccessFileReader;
import cn.edu.tsinghua.tsfile.file.metadata.TimeSeriesMetadata;
import cn.edu.tsinghua.tsfile.file.metadata.RowGroupMetaData;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.FilterFactory;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.SingleSeriesFilterExpression;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.filterseries.FilterSeries;
import cn.edu.tsinghua.tsfile.timeseries.filter.definition.filterseries.FilterSeriesType;
import cn.edu.tsinghua.tsfile.timeseries.read.management.SeriesSchema;
import cn.edu.tsinghua.tsfile.timeseries.read.query.DynamicOneColumnData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

/**
 * This class implements several read methods which can read data in different ways.<br>
 * This class provides some APIs for reading.
 */
public class RecordReader {

    private static final Logger logger = LoggerFactory.getLogger(RecordReader.class);

    /** corresponding FileReader **/
    private FileReader fileReader;
    private Map<String, Map<String, SeriesSchema>> seriesSchemaMap;

    /**
     * init {@code fileReader}
     * @param raf input tsfile reader
     * @throws IOException
     */
    public RecordReader(ITsRandomAccessFileReader raf) throws IOException {
        this.fileReader = new FileReader(raf);
    }

    /**
     * init {@code fileReader} by tsfile reader and list of RowGroupMetaDatas
     * for hadoop-connector
     */
    public RecordReader(ITsRandomAccessFileReader raf, List<RowGroupMetaData> rowGroupMetaDataList) throws IOException {
        this.fileReader = new FileReader(raf, rowGroupMetaDataList);
    }

    /**
     * Read one path without filter.
     *
     * @param res the iterative result
     * @param fetchSize fetch size
     * @param deltaObjectUID delta object Id
     * @param measurementUID measurement Id
     * @return the result in means of DynamicOneColumnData
     * @throws IOException TsFile read error
     */
    public DynamicOneColumnData getValueInOneColumn(DynamicOneColumnData res, int fetchSize
            , String deltaObjectUID, String measurementUID) throws IOException {

        // make sure deltaObjectUID and measurementUID exists
        checkSeries(deltaObjectUID, measurementUID);

        // get corresponding list of RowGroupReaders of input {@code deltaObjectUID}
        List<RowGroupReader> rowGroupReaderList = fileReader.getRowGroupReaderListByDeltaObject(deltaObjectUID);
        // get current RowGroup index
        int i = 0;
        if (res != null) {
            i = res.getRowGroupIndex();
        }
        // loop rest RowGroupReaders
        for (; i < rowGroupReaderList.size(); i++) {
            // get next RowGroupReader
            RowGroupReader rowGroupReader = rowGroupReaderList.get(i);

            // if corresponding ValueReader of input {@code measurementUID} does not exist,
            // return a new DynamicOneColumnData for input {@code measurementUID}
            if(rowGroupReader.getValueReaders().get(measurementUID) == null) {
                return alignColumn(measurementUID);
            }

            // read one column from reader of {@code measurementUID}
            res = getValueInOneColumn(res, fetchSize, rowGroupReader, measurementUID);
            if (res.valueLength >= fetchSize) {
                res.hasReadAll = false;
                break;
            }
        }
        return res;
    }

    /**
     * Read one path without filter and do not throw exception. Used by hadoop.
     *
     * @param res the iterative result
     * @param fetchSize fetch size
     * @param deltaObjectUID delta object id
     * @param measurementUID  measurement Id
     * @return the result in means of DynamicOneColumnData
     * @throws IOException TsFile read error
     */
    public DynamicOneColumnData getValueInOneColumnWithoutException(DynamicOneColumnData res, int fetchSize
            , String deltaObjectUID, String measurementUID) throws IOException {
        try {
            // check if {@code deltaObjectUID} and {@code measurementUID} exist
            checkSeriesByHadoop(deltaObjectUID, measurementUID);
        }catch(IOException ex){
            // if not exist, create a new DynamicOneColumnData and set its dataType and return it
            if(res == null)res = new DynamicOneColumnData();
            res.dataType = fileReader.getRowGroupReaderListByDeltaObject(deltaObjectUID).get(0).getDataTypeBySeriesName(measurementUID);
            return res;
        }
        // get corresponding list of RowGroupReaders of input {@code deltaObjectUID}
        List<RowGroupReader> rowGroupReaderList = fileReader.getRowGroupReaderListByDeltaObjectByHadoop(deltaObjectUID);
        // get current RowGroup index
        int i = 0;
        if (res != null) {
            i = res.getRowGroupIndex();
        }
        // loop rest RowGroupReaders
        for (; i < rowGroupReaderList.size(); i++) {
            // get next RowGroupReader
            RowGroupReader rowGroupReader = rowGroupReaderList.get(i);

            // read one column from reader of {@code measurementUID}
            res = getValueInOneColumn(res, fetchSize, rowGroupReader, measurementUID);
            if (res.valueLength >= fetchSize) {
                res.hasReadAll = false;
                break;
            }
        }
        return res;
    }

    /**
     * read one column from ValueReader of {@code measurementId}
     * @param res
     * @param fetchSize
     * @param rowGroupReader
     * @param measurementId
     * @return
     * @throws IOException
     */
    private DynamicOneColumnData getValueInOneColumn(DynamicOneColumnData res, int fetchSize,
                                                     RowGroupReader rowGroupReader, String measurementId) throws IOException {
        return rowGroupReader.getValueReaders().get(measurementId).readOneColumn(res, fetchSize);
    }


    /**
     * Read one path without filter from one specific <code>RowGroupReader</code> according to the indexList
     * @param res result
     * @param fetchSize fetch size
     * @param deltaObjectUID delta object id
     * @param measurementId  measurement Id
     * @param indexes index list of the RowGroupReader
     * @return DynamicOneColumnData
     * @throws IOException failed to get value
     */
    public DynamicOneColumnData getValueInOneColumn(DynamicOneColumnData res, int fetchSize, String deltaObjectUID,
                                                    String measurementId, ArrayList<Integer> indexes) throws IOException {
        // check if {@code deltaObjectUID} and {@code measurementUID} exist
        checkSeries(deltaObjectUID, measurementId);
        int rowGroupSkipCount = 0;

        // get corresponding list of RowGroupReaders of input {@code deltaObjectUID}
        List<RowGroupReader> rowGroupReaderList = fileReader.getRowGroupReaderList();
        // get current RowGroup index
        int i = 0;
        if (res != null) {
            i = res.getRowGroupIndex();
        }
        // loop rest RowGroupReaders according to {@code indexes}
        for (; i < indexes.size(); i++) {
            // get next RowGroupReader
            int idx = indexes.get(i);
            RowGroupReader rowGroupReader = rowGroupReaderList.get(idx);
            // if current RowGroupReader not belong to {@code deltaObjectUID}, skip it
            if (!deltaObjectUID.equals(rowGroupReader.getDeltaObjectUID())) {
                rowGroupSkipCount++;
                continue;
            }

            // if corresponding ValueReader of input {@code measurementUID} does not exist,
            // return a new DynamicOneColumnData for input {@code measurementUID}
            if(rowGroupReader.getValueReaders().get(measurementId) == null) {
                return alignColumn(measurementId);
            }

            // read one column from reader of {@code measurementUID}
            res = rowGroupReader.getValueReaders().get(measurementId).readOneColumn(res, fetchSize);
            // add skipped num to result
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

    /**
     * Read one path with value filter
     * @param res
     * @param fetchSize
     * @param valueFilter
     * @return
     * @throws IOException
     */
    public DynamicOneColumnData getValuesUseFilter(DynamicOneColumnData res, int fetchSize
            , SingleSeriesFilterExpression valueFilter) throws IOException {
        // get deltaObjectUID and measurementUID from {@code valueFilter}
        String deltaObjectUID = valueFilter.getFilterSeries().getDeltaObjectUID();
        String measurementUID = valueFilter.getFilterSeries().getMeasurementUID();
        return getValuesUseFilter(res, fetchSize, deltaObjectUID, measurementUID, null, null, valueFilter);
    }

    /**
     * Read one path with time filter, frequency filter and value filter
     * @param res
     * @param fetchSize
     * @param deltaObjectUID
     * @param measurementId
     * @param timeFilter
     * @param freqFilter
     * @param valueFilter
     * @return
     * @throws IOException
     */
    public DynamicOneColumnData getValuesUseFilter(DynamicOneColumnData res, int fetchSize, String deltaObjectUID,
                                                   String measurementId, SingleSeriesFilterExpression timeFilter, SingleSeriesFilterExpression freqFilter,
                                                   SingleSeriesFilterExpression valueFilter) throws IOException {
        // check if {@code deltaObjectUID} and {@code measurementUID} exist
        checkSeries(deltaObjectUID, measurementId);

        // get current RowGroup index
        int i = 0;
        if (res != null) {
            i = res.getRowGroupIndex();
        }

        // get corresponding list of RowGroupReaders of input {@code deltaObjectUID}
        List<RowGroupReader> rowGroupReaderList = fileReader.getRowGroupReaderListByDeltaObject(deltaObjectUID);
        // loop rest RowGroupReaders
        for (; i < rowGroupReaderList.size(); i++) {
            // get next RowGroupReader
            RowGroupReader rowGroupReader = rowGroupReaderList.get(i);

            // if corresponding ValueReader of input {@code measurementUID} does not exist,
            // return a new DynamicOneColumnData for input {@code measurementUID}
            if(rowGroupReader.getValueReaders().get(measurementId) == null) {
                return alignColumn(measurementId);
            }

            // read one column from current RowGroupReader with filters
            res = getValuesUseFilter(res, fetchSize, rowGroupReader, measurementId, timeFilter, freqFilter, valueFilter);
            if (res.valueLength >= fetchSize) {
                res.hasReadAll = false;
                break;
            }
        }
        return res;
    }

    /**
     * Read one path with value filter according to {@code idxs}
     * @param res
     * @param fetchSize
     * @param valueFilter
     * @param idxs
     * @return
     * @throws IOException
     */
    public DynamicOneColumnData getValuesUseFilter(DynamicOneColumnData res, int fetchSize
            , SingleSeriesFilterExpression valueFilter, ArrayList<Integer> idxs) throws IOException {
        String deltaObjectUID = valueFilter.getFilterSeries().getDeltaObjectUID();
        String measurementUID = valueFilter.getFilterSeries().getMeasurementUID();
        return getValuesUseFilter(res, fetchSize, deltaObjectUID, measurementUID, null, null, valueFilter, idxs);
    }

    /**
     * Read one path with time, frequency and value filter according to {@code idxs}
     * @param res
     * @param fetchSize
     * @param deltaObjectUID
     * @param measurementId
     * @param timeFilter
     * @param freqFilter
     * @param valueFilter
     * @param idxs
     * @return
     * @throws IOException
     */
    public DynamicOneColumnData getValuesUseFilter(DynamicOneColumnData res, int fetchSize, String deltaObjectUID,
                                                   String measurementId, SingleSeriesFilterExpression timeFilter, SingleSeriesFilterExpression freqFilter,
                                                   SingleSeriesFilterExpression valueFilter, ArrayList<Integer> idxs) throws IOException {
        // check if {@code deltaObjectUID} and {@code measurementUID} exist
        checkSeries(deltaObjectUID, measurementId);
        int rowGroupSkipCount = 0;

        // get corresponding list of RowGroupReaders of input {@code deltaObjectUID}
        List<RowGroupReader> rowGroupReaderList = fileReader.getRowGroupReaderList();
        // get current RowGroup index
        int i = 0;
        if (res != null) {
            i = res.getRowGroupIndex();
        }
        // loop rest RowGroupReaders according to {@code indexes}
        for (; i < idxs.size(); i++) {
            logger.info("GetValuesUseFilter and timeIdxs. RowGroupIndex is :" + idxs.get(i));
            // get next RowGroupReader
            int idx = idxs.get(i);
            RowGroupReader rowGroupReader = rowGroupReaderList.get(idx);
            // if current RowGroupReader not belong to {@code deltaObjectUID}, skip it
            if (!deltaObjectUID.equals(rowGroupReader.getDeltaObjectUID())) {
                rowGroupSkipCount++;
                continue;
            }

            // if corresponding ValueReader of input {@code measurementUID} does not exist,
            // return a new DynamicOneColumnData for input {@code measurementUID}
            if(rowGroupReader.getValueReaders().get(measurementId) == null) {
                return alignColumn(measurementId);
            }

            // read one column from current RowGroupReader with filters
            res = getValuesUseFilter(res, fetchSize, rowGroupReader, measurementId, timeFilter, freqFilter, valueFilter);
            // add skipped num to result
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

    /**
     * Read one path with time, frequency and value filter from {@code rowGroupReader}
     * @param res
     * @param fetchSize
     * @param rowGroupReader
     * @param measurementId
     * @param timeFilter
     * @param freqFilter
     * @param valueFilter
     * @return
     * @throws IOException
     */
    private DynamicOneColumnData getValuesUseFilter(DynamicOneColumnData res, int fetchSize,
                                                    RowGroupReader rowGroupReader, String measurementId, SingleSeriesFilterExpression timeFilter,
                                                    SingleSeriesFilterExpression freqFilter, SingleSeriesFilterExpression valueFilter) throws IOException {

        res = rowGroupReader.getValueReaders().get(measurementId).readOneColumnUseFilter(res, fetchSize, timeFilter,
                freqFilter, valueFilter);
        return res;
    }

    /**
     * Read one path according to input timestamp list
     * @param deltaObjectUID
     * @param measurementId
     * @param timestamps
     * @return
     * @throws IOException
     */
    public DynamicOneColumnData getValuesUseTimestamps(String deltaObjectUID, String measurementId, long[] timestamps)
            throws IOException {
        // check if {@code deltaObjectUID} and {@code measurementUID} exist
        checkSeries(deltaObjectUID, measurementId);
        DynamicOneColumnData res = null;
        // get corresponding list of RowGroupReaders of input {@code deltaObjectUID}
        List<RowGroupReader> rowGroupReaderList = fileReader.getRowGroupReaderListByDeltaObject(deltaObjectUID);
        for (int i = 0; i < rowGroupReaderList.size(); i++) {
            // get next RowGroupReader
            RowGroupReader rowGroupReader = rowGroupReaderList.get(i);

            // if corresponding ValueReader of input {@code measurementUID} does not exist,
            // return a new DynamicOneColumnData for input {@code measurementUID}
            if(rowGroupReader.getValueReaders().get(measurementId) == null) {
                return alignColumn(measurementId);
            }

            // read values
            if (i == 0) {
                res = getValuesUseTimestamps(rowGroupReader, measurementId, timestamps);
            } else {
                DynamicOneColumnData tmpRes = getValuesUseTimestamps(rowGroupReader, measurementId, timestamps);
                res.mergeRecord(tmpRes);
            }
        }
        return res;
    }

    /**
     * Read one path according to input timestamp list and index list
     * @param deltaObjectUID
     * @param measurementId
     * @param timeRet
     * @param idxs
     * @return
     * @throws IOException
     */
    public DynamicOneColumnData getValuesUseTimestamps(String deltaObjectUID, String measurementId, long[] timeRet,
                                                       ArrayList<Integer> idxs) throws IOException {
        // check if {@code deltaObjectUID} and {@code measurementUID} exist
        checkSeries(deltaObjectUID, measurementId);
        DynamicOneColumnData res = null;
        // get corresponding list of RowGroupReaders of input {@code deltaObjectUID}
        List<RowGroupReader> rowGroupReaderList = fileReader.getRowGroupReaderList();

        boolean init = false;
        for (int i = 0; i < idxs.size(); i++) {
            int idx = idxs.get(i);
            // get next RowGroupReader
            RowGroupReader rowGroupReader = rowGroupReaderList.get(idx);
            // if current RowGroupReader not belong to {@code deltaObjectUID}, skip it
            if (!deltaObjectUID.equals(rowGroupReader.getDeltaObjectUID())) {
                continue;
            }

            // if corresponding ValueReader of input {@code measurementUID} does not exist,
            // return a new DynamicOneColumnData for input {@code measurementUID}
            if(rowGroupReader.getValueReaders().get(measurementId) == null) {
                return alignColumn(measurementId);
            }

            // read values
            if (!init) {
                res = getValuesUseTimestamps(rowGroupReader, measurementId, timeRet);
                init = true;
            } else {
                DynamicOneColumnData tmpRes = getValuesUseTimestamps(rowGroupReader, measurementId, timeRet);
                res.mergeRecord(tmpRes);
            }
        }
        return res;
    }

    /**
     * Read one path according to input timestamp list from {@code rowGroupReader}
     * @param rowGroupReader
     * @param measurementId
     * @param timeRet
     * @return
     * @throws IOException
     */
    private DynamicOneColumnData getValuesUseTimestamps(RowGroupReader rowGroupReader, String measurementId, long[] timeRet)
            throws IOException {
        return rowGroupReader.getValueReaders().get(measurementId).getValuesForGivenValues(timeRet);
    }

    public boolean isEnumsColumn(String deltaObjectUID, String sid) throws IOException {
        List<RowGroupReader> rowGroupReaderList = fileReader.getRowGroupReaderListByDeltaObject(deltaObjectUID);
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

    /**
     * get all SeriesSchemas from {@code fileReader}
     * for Tsfile-Spark-Connector
     *
     * @return
     * @throws IOException
     */
    public List<SeriesSchema> getAllSeriesSchema() throws IOException {
        List<TimeSeriesMetadata> tslist = this.fileReader.getFileMetaData().getTimeSeriesList();
        List<SeriesSchema> seriesSchemas = new ArrayList<>();
        for(TimeSeriesMetadata ts: tslist ) {
            seriesSchemas.add(new SeriesSchema(ts.getMeasurementUID(), ts.getType(), null));
        }
        return seriesSchemas;
    }

    /**
     * get all deltaObjectIds from {@code fileReader}
     * @return
     * @throws IOException
     */
    public ArrayList<String> getAllDeltaObjects() throws IOException {
        ArrayList<String> res = new ArrayList<>();
        HashMap<String, Integer> deltaObjectMap = new HashMap<>();
        List<RowGroupReader> rowGroupReaders = fileReader.getRowGroupReaderList();
        for (RowGroupReader rgr : rowGroupReaders) {
            String deltaObjectUID = rgr.getDeltaObjectUID();
            if (!deltaObjectMap.containsKey(deltaObjectUID)) {
                res.add(deltaObjectUID);
                deltaObjectMap.put(deltaObjectUID, 1);
            }
        }
        return res;
    }

    /**
     * init get All Map<deltaObjectUID, List<SeriesSchema>> from RowGroupReaders of {@code fileReader}
     * @return
     */
    public Map<String, ArrayList<SeriesSchema>> getAllSeriesSchemasGroupByDeltaObject() {
        Map<String, ArrayList<SeriesSchema>> res = new HashMap<>();
        // get all RowGroupReaders
        Map<String, List<RowGroupReader>> rowGroupReaders = fileReader.getRowGroupReaderMap();
        // loop all deltaObjectUIDs
        for (String deltaObjectUID : rowGroupReaders.keySet()) {
            HashMap<String, Integer> measurementMap = new HashMap<>();
            ArrayList<SeriesSchema> cols = new ArrayList<>();
            // loop all corresponding RowGroupReaders of current deltaObjectUID and get all corresponding SeriesSchemas
            for (RowGroupReader rgr : rowGroupReaders.get(deltaObjectUID)) {
                // loop all measurementIds of current RowGroupReader
                for (String measurement : rgr.seriesDataTypeMap.keySet()) {
                    // if current measurementId not exist, update {@code cols}
                    if (!measurementMap.containsKey(measurement)) {
                        cols.add(new SeriesSchema(measurement, rgr.seriesDataTypeMap.get(measurement), null));
                        measurementMap.put(measurement, 1);
                    }
                }
            }
            // update result
            res.put(deltaObjectUID, cols);
        }
        return res;
    }

    /**
     * get Map<deltaObjectId, RowGroups num>
     * @return
     */
    public Map<String, Integer> getDeltaObjectRowGroupCounts() {
        Map<String, Integer> res = new HashMap<>();
        Map<String, List<RowGroupReader>> rowGroupReaders = fileReader.getRowGroupReaderMap();
        for (String deltaObjectUID : rowGroupReaders.keySet()) {
            res.put(deltaObjectUID, rowGroupReaders.get(deltaObjectUID).size());
        }
        return res;
    }

    /**
     * get all deltaObject types
     * @return
     */
    public Map<String, String> getDeltaObjectTypes() {
        Map<String, String> res = new HashMap<>();
        Map<String, List<RowGroupReader>> rowGroupReaders = fileReader.getRowGroupReaderMap();
        for (String deltaObjectUID : rowGroupReaders.keySet()) {

            RowGroupReader rgr = rowGroupReaders.get(deltaObjectUID).get(0);
        }
        return res;
    }

    /**
     * get all start positions of RowGroup
     * @return
     * @throws IOException
     */
    public ArrayList<Long> getRowGroupPosList() throws IOException {
        ArrayList<Long> res = new ArrayList<>();
        long startPos = 0;
        for (RowGroupReader rowGroupReader : fileReader.getRowGroupReaderList()) {
            long currentEndPos = rowGroupReader.getTotalByteSize() + startPos;
            res.add(currentEndPos);
            startPos = currentEndPos;
        }
        return res;
    }

    /**
     * get corresponding FilterSeries of {@code deltaObject} and {@code measurement}
     * @param deltaObject
     * @param measurement
     * @return
     * @throws IOException
     */
    public FilterSeries<?> getColumnByMeasurementName(String deltaObject, String measurement) throws IOException {
        TSDataType type = null;

        //modified for Tsfile-Spark-Connector
        type = this.fileReader.getFileMetaData().getType(measurement);

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

    /**
     * make sure input deltaObject and measurement are in the {@code fileReader}
     * modified for Tsfile-Spark-Connector
     *
     * @param deltaObject
     * @param measurement
     * @throws IOException
     */
    private void checkSeries(String deltaObject, String measurement) throws IOException {
        this.fileReader.loadDeltaObj(deltaObject);
        if(!fileReader.containsDeltaObj(deltaObject) || !fileReader.getFileMetaData().containsMeasurement(measurement)) {
            throw new IOException("Series "+ deltaObject + "#" + measurement + " does not exist in the current file.");
        }
    }


    /**
     * create a new DynamicOneColumnData for input {@code measurementId}
     * corresponding with the modification of method 'checkSeries'
     *
     * @param measurementId
     * @return new DynamicOneColumnData
     * @throws IOException
     */
    private DynamicOneColumnData alignColumn(String measurementId) throws IOException{
        TSDataType type = fileReader.getFileMetaData().getType(measurementId);
        return new DynamicOneColumnData(type);
    }

    /**
     * make sure input deltaObject and measurement are in the {@code seriesSchemaMap}
     * @param deltaObject
     * @param measurement
     * @throws IOException
     */
    private void checkSeriesByHadoop(String deltaObject, String measurement) throws IOException {
        // if {@code seriesSchemaMap} is null, init it
        if (seriesSchemaMap == null) {
            seriesSchemaMap = new HashMap<>();
            // get all Map<deltaObjectId, ArrayList<SeriesSchema>>
            Map<String, ArrayList<SeriesSchema>> seriesSchemaListMap = getAllSeriesSchemasGroupByDeltaObject();
            // loop all deltaObjectIds
            for (String key : seriesSchemaListMap.keySet()) {
                // get all SeriesSchemas of current deltaObjectId and update {@code seriesSchemaMap}
                HashMap<String, SeriesSchema> tmap = new HashMap<>();
                for (SeriesSchema ss : seriesSchemaListMap.get(key)) {
                    tmap.put(ss.name, ss);
                }
                seriesSchemaMap.put(key, tmap);
            }
        }
        // if {@code seriesSchemaMap} not contain {@code deltaObject} or {@code measurement},
        // throw IOException
        if (seriesSchemaMap.containsKey(deltaObject)) {
            if (seriesSchemaMap.get(deltaObject).containsKey(measurement)) {
                return;
            }
        }
        throw new IOException("Series is not exist in current file: " + deltaObject + "#" + measurement);
    }

    public List<RowGroupReader> getAllRowGroupReaders() throws IOException {
        return fileReader.getRowGroupReaderList();
    }

    public Map<String, String> getProps() {
        return fileReader.getProps();
    }

    public String getProp(String key) {
        return fileReader.getProp(key);
    }

    /**
     * close {@code fileReader}
     * @throws IOException
     */
    public void close() throws IOException {
        fileReader.close();
    }


}

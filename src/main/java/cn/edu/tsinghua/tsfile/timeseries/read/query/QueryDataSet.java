package cn.edu.tsinghua.tsfile.timeseries.read.query;

import cn.edu.tsinghua.tsfile.common.exception.UnSupportedDataTypeException;
import cn.edu.tsinghua.tsfile.timeseries.read.support.Field;
import cn.edu.tsinghua.tsfile.timeseries.read.support.RowRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.PriorityQueue;

public class QueryDataSet {
    protected static final Logger LOG = LoggerFactory.getLogger(QueryDataSet.class);
    protected static final char PATH_SPLITTER = '.';

    /**
     * Time Generator for Cross Query when using batching read
     **/
    public CrossQueryTimeGenerator crossQueryTimeGenerator;

    /**
     * mapRet.key stores the query path, mapRet.value stores the query result of mapRet.key
     **/
    public LinkedHashMap<String, DynamicOneColumnData> mapRet;

    /**
     * generator used for batch read
     **/
    protected BatchReadRecordGenerator batchReadGenerator;

    /**
     * special for save time values when processing cross getIndex
     **/
    protected PriorityQueue<Long> heap;

    /**
     * the content of cols equals to mapRet
     **/
    protected DynamicOneColumnData[] cols;

    /**
     * timeIdxs[i] stores the index of cols[i]
     **/
    protected int[] timeIdxs;

    /**
     * emptyTimeIdxs[i] stores the empty time index of cols[i]
     **/
    protected int[] emptyTimeIdxs;

    protected String[] deltaObjectIds;
    protected String[] measurementIds;
    protected HashMap<Long, Integer> timeMap; // timestamp occurs time
    protected int size;
    protected boolean ifInit = false;
    protected RowRecord currentRecord = null;
    private Map<String, Object> deltaMap; // this variable is used for IoTDb

    public QueryDataSet() {
        mapRet = new LinkedHashMap<>();
    }

    public void initForRecord() {
        size = mapRet.keySet().size();

        if (size > 0) {
            heap = new PriorityQueue<>(size);
            cols = new DynamicOneColumnData[size];
            deltaObjectIds = new String[size];
            measurementIds = new String[size];
            timeIdxs = new int[size];
            emptyTimeIdxs = new int[size];
            timeMap = new HashMap<>();
        } else {
            LOG.error("QueryDataSet init row record occurs error! the size of ret is 0.");
            heap = new PriorityQueue<>();
        }

        int i = 0;
        for (String key : mapRet.keySet()) {
            cols[i] = mapRet.get(key);
            deltaObjectIds[i] = key.substring(0, key.lastIndexOf(PATH_SPLITTER));
            measurementIds[i] = key.substring(key.lastIndexOf(PATH_SPLITTER) + 1);
            timeIdxs[i] = 0;
            emptyTimeIdxs[i] = 0;

            if (cols[i] != null && (cols[i].valueLength > 0 || cols[i].timeLength > 0 || cols[i].emptyTimeLength > 0)) {
                long minTime = Long.MAX_VALUE;
                if (cols[i].timeLength > 0) {
                    minTime = cols[i].getTime(0);
                }
                if (cols[i].emptyTimeLength > 0) {
                    minTime = Math.min(minTime, cols[i].getEmptyTime(0));
                }
                heapPut(minTime);
            }
            i++;
        }
    }

    protected void heapPut(long t) {
        if (!timeMap.containsKey(t)) {
            heap.add(t);
            timeMap.put(t, 1);
        }
    }

    protected Long heapGet() {
        Long t = heap.poll();
        timeMap.remove(t);
        return t;
    }

    public boolean hasNextRecord() {
        if (!ifInit) {
            initForRecord();
            ifInit = true;
        }
        if (heap.peek() != null) {
            return true;
        }
        return false;
    }

    public RowRecord getNextRecord() {
        if (!ifInit) {
            initForRecord();
            ifInit = true;
        }

        Long minTime = heapGet();
        if (minTime == null) {
            return null;
        }

        RowRecord record = new RowRecord(minTime, null, null);
        for (int i = 0; i < size; i++) {
            if (i == 0) {
                record.setDeltaObjectId(deltaObjectIds[i]);
            }
            Field field = new Field(cols[i].dataType, deltaObjectIds[i], measurementIds[i]);
            if (timeIdxs[i] < cols[i].timeLength && minTime == cols[i].getTime(timeIdxs[i])) {
                field.setNull(false);
                putValueToField(cols[i], timeIdxs[i], field);
                timeIdxs[i]++;
                long nextTime = Long.MAX_VALUE;
                if (timeIdxs[i] < cols[i].timeLength) {
                    nextTime = cols[i].getTime(timeIdxs[i]);
                }
                if (emptyTimeIdxs[i] < cols[i].emptyTimeLength) {
                    nextTime = Math.min(nextTime, cols[i].getEmptyTime(emptyTimeIdxs[i]));
                }
                if (nextTime != Long.MAX_VALUE) {
                    heapPut(nextTime);
                }
            } else if (emptyTimeIdxs[i] < cols[i].emptyTimeLength && minTime == cols[i].getEmptyTime(emptyTimeIdxs[i])) {
                field.setNull(true);
                emptyTimeIdxs[i]++;
                long nextTime = Long.MAX_VALUE;
                if (emptyTimeIdxs[i] < cols[i].emptyTimeLength) {
                    nextTime = cols[i].getEmptyTime(emptyTimeIdxs[i]);
                }
                if (timeIdxs[i] < cols[i].timeLength) {
                    nextTime = Math.min(nextTime, cols[i].getTime(timeIdxs[i]));
                }
                if (nextTime != Long.MAX_VALUE) {
                    heapPut(nextTime);
                }
            } else {
                field.setNull(true);
            }
            record.addField(field);
        }
        return record;
    }

    public boolean next() {
        if (hasNextRecord()) {
            currentRecord = getNextRecord();
            return true;
        }
        currentRecord = null;
        return false;
    }

    public RowRecord getCurrentRecord() {
        if (!ifInit) {
            initForRecord();
            ifInit = true;
        }
        return currentRecord;
    }

    public void putValueToField(DynamicOneColumnData col, int idx, Field f) {
        switch (col.dataType) {
            case BOOLEAN:
                f.setBoolV(col.getBoolean(idx));
                break;
            case INT32:
                f.setIntV(col.getInt(idx));
                break;
            case INT64:
                f.setLongV(col.getLong(idx));
                break;
            case FLOAT:
                f.setFloatV(col.getFloat(idx));
                break;
            case DOUBLE:
                f.setDoubleV(col.getDouble(idx));
                break;
            case TEXT:
                f.setBinaryV(col.getBinary(idx));
                break;
            default:
                throw new UnSupportedDataTypeException("UnSupported" + String.valueOf(col.dataType));
        }
    }

    public void clear() {
        this.ifInit = false;
        for (DynamicOneColumnData col : mapRet.values()) {
            col.clearData();
        }
    }

    public BatchReadRecordGenerator getBatchReadGenerator() {
        return batchReadGenerator;
    }

    public void setBatchReadGenerator(BatchReadRecordGenerator batchReadGenerator) {
        this.batchReadGenerator = batchReadGenerator;
    }

    public Map<String, Object> getDeltaMap() {
        return this.deltaMap;
    }

    public void setDeltaMap(Map<String, Object> deltaMap) {
        this.deltaMap = deltaMap;
    }
}
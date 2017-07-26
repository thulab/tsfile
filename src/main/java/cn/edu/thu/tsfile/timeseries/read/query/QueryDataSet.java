package cn.edu.thu.tsfile.timeseries.read.query;

import cn.edu.thu.tsfile.common.exception.UnSupportedDataTypeException;
import cn.edu.thu.tsfile.timeseries.read.qp.Path;
import cn.edu.thu.tsfile.timeseries.read.support.Field;
import cn.edu.thu.tsfile.timeseries.read.support.RowRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.PriorityQueue;


public class QueryDataSet {
    private static final Logger LOG = LoggerFactory.getLogger(QueryDataSet.class);
    private static final char PATH_SPLITTER = '.';

    //Time Generator for Cross Query when using batching read
    public CrossQueryTimeGenerator timeQueryDataSet;
    public LinkedHashMap<String, DynamicOneColumnData> mapRet;
    //TODO this variable need to be set in TsFileDB
    protected BatchReadRecordGenerator batchReaderRetGenerator;
    //special for save time values when processing cross getIndex
    protected PriorityQueue<Long> heap;
    protected DynamicOneColumnData[] cols;
    protected String[] deltaObjectIds;
    protected String[] measurementIds;
    protected int[] idxs;
    // timestamp occurs time
    protected HashMap<Long, Integer> timeMap;
    protected int size;
    protected boolean ifInit = false;
    protected RowRecord currentRecord = null;
    private Map<String, Object> deltaMap; // this variable is used for TsFileDb

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
            idxs = new int[size];
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
            idxs[i] = 0;

            if (cols[i] != null && (cols[i].valueLength > 0 || cols[i].timeLength > 0)) {
                heapPut(cols[i].getTime(0));
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

        RowRecord r = new RowRecord(minTime, null, null);
        for (int i = 0; i < size; i++) {
            if (i == 0) {
                r.setDeltaObjectId(deltaObjectIds[i]);
                r.setDeltaObjectType(cols[i].getDeltaObjectType());
            }
            Field f = new Field(cols[i].dataType, deltaObjectIds[i], measurementIds[i]);

            if (idxs[i] < cols[i].valueLength && minTime == cols[i].getTime(idxs[i])) {
                // f = new Field(cols[i].dataType, deltaObjectIds[i], measurementIds[i]);
                f.setNull(false);
                putValueToField(cols[i], idxs[i], f);
                idxs[i]++;
                if (idxs[i] < cols[i].valueLength) {
                    heapPut(cols[i].getTime(idxs[i]));
                }
            } else {
                // f = new Field(cols[i].dataType, measurementIds[i]);
                f.setNull(true);
            }
            r.addField(f);
        }
        return r;
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
            case ENUMS:
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

    //TODO this method need to be removed
    public void putRecordFromBatchReadRetGenerator() {
        for (Path p : getBatchReaderRetGenerator().retMap.keySet()) {
            DynamicOneColumnData oneColRet = getBatchReaderRetGenerator().retMap.get(p);
            DynamicOneColumnData leftRet = oneColRet.sub(oneColRet.curIdx);
            leftRet.setDeltaObjectType(oneColRet.getDeltaObjectType());
            //Copy batch read info from oneColRet to leftRet
            oneColRet.copyFetchInfoTo(leftRet);
            getBatchReaderRetGenerator().retMap.put(p, leftRet);
            oneColRet.rollBack(oneColRet.valueLength - oneColRet.curIdx);
            this.mapRet.put(p.getFullPath(), oneColRet);
        }
    }

    public BatchReadRecordGenerator getBatchReaderRetGenerator() {
        return batchReaderRetGenerator;
    }

    public void setBatchReaderRetGenerator(BatchReadRecordGenerator batchReaderRetGenerator) {
        this.batchReaderRetGenerator = batchReaderRetGenerator;
    }

    public Map<String, Object> getDeltaMap() {
        return this.deltaMap;
    }

    public void setDeltaMap(Map<String, Object> deltaMap) {
        this.deltaMap = deltaMap;
    }
}
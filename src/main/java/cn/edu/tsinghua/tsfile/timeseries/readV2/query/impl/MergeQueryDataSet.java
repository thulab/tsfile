package cn.edu.tsinghua.tsfile.timeseries.readV2.query.impl;

import cn.edu.tsinghua.tsfile.common.exception.UnSupportedDataTypeException;
import cn.edu.tsinghua.tsfile.timeseries.read.query.DynamicOneColumnData;
import cn.edu.tsinghua.tsfile.timeseries.read.support.FieldV1;
import cn.edu.tsinghua.tsfile.timeseries.read.support.OldRowRecord;
import cn.edu.tsinghua.tsfile.timeseries.read.support.Path;
import cn.edu.tsinghua.tsfile.timeseries.readV2.datatype.RowRecord;
import cn.edu.tsinghua.tsfile.timeseries.readV2.datatype.TimeValuePair;
import cn.edu.tsinghua.tsfile.timeseries.readV2.datatype.TsPrimitiveType;
import cn.edu.tsinghua.tsfile.timeseries.readV2.query.QueryDataSet;
import cn.edu.tsinghua.tsfile.timeseries.readV2.reader.SeriesReader;

import java.io.IOException;
import java.util.*;

/**
 * Created by zhangjinrui on 2017/12/27.
 */
public class MergeQueryDataSet implements QueryDataSet {

    /** ======================  Old version using TsPrimitive ====================== **/
    private LinkedHashMap<Path, SeriesReader> readersOfSelectedSeries;
    private PriorityQueue<Point> heap;

    public MergeQueryDataSet(LinkedHashMap<Path, SeriesReader> readersOfSelectedSeries) throws IOException {
        this.readersOfSelectedSeries = readersOfSelectedSeries;
        initHeap();
    }

    private void initHeap() throws IOException {
        heap = new PriorityQueue<>();
        for (Path path : readersOfSelectedSeries.keySet()) {
            SeriesReader seriesReader = readersOfSelectedSeries.get(path);
            if (seriesReader.hasNext()) {
                TimeValuePair timeValuePair = seriesReader.next();
                heap.add(new Point(path, timeValuePair.getTimestamp(), timeValuePair.getValue()));
            }
        }
    }

    @Override
    public boolean hasNext() throws IOException {
        return timeHeap.size() > 0;
    }

    @Override
    public RowRecord next() throws IOException {
        Point aimPoint = heap.peek();
        RowRecord rowRecord = new RowRecord(aimPoint.timestamp);
        for (Path path : readersOfSelectedSeries.keySet()) {
            rowRecord.putField(path, null);
        }
        while (heap.size() > 0 && heap.peek().timestamp == aimPoint.timestamp) {
            Point point = heap.poll();
            rowRecord.putField(point.path, point.tsPrimitiveType);
            if (readersOfSelectedSeries.get(point.path).hasNext()) {
                TimeValuePair nextTimeValuePair = readersOfSelectedSeries.get(point.path).next();
                heap.add(new Point(point.path, nextTimeValuePair.getTimestamp(), nextTimeValuePair.getValue()));
            }
        }
        return rowRecord;
    }

    private static class Point implements Comparable<Point> {
        private Path path;
        private long timestamp;
        private TsPrimitiveType tsPrimitiveType;

        private Point(Path path, long timestamp, TsPrimitiveType tsPrimitiveType) {
            this.path = path;
            this.timestamp = timestamp;
            this.tsPrimitiveType = tsPrimitiveType;
        }

        @Override
        public int compareTo(Point o) {
            return Long.compare(timestamp, o.timestamp);
        }
    }






    /** ====================== New version using Dynamic ============================= **/

    public LinkedHashMap<Path, DynamicOneColumnData> seriesDataMap;
    protected DynamicOneColumnData[] seriesDataArray;

    // usingIdxsOfSeriesData[i] stores the using index of seriesDataArray[i]
    protected int[] usingIdxsOfSeriesData;

    private Map<Path, Boolean> hasDataRemaining;

    // heap only need to store
    private PriorityQueue<Long> timeHeap;

    private Set<Long> timeSet;

    public MergeQueryDataSet(LinkedHashMap<Path, SeriesReader> readersOfSelectedSeries, boolean flag) throws IOException {
        this.readersOfSelectedSeries = readersOfSelectedSeries;
        initHeapV2();
    }

    private void initHeapV2() throws IOException {
        hasDataRemaining = new HashMap<>();
        seriesDataMap = new LinkedHashMap<>();
        timeHeap = new PriorityQueue<>();
        timeSet = new HashSet<>();

        for (Path path : readersOfSelectedSeries.keySet()) {
            SeriesReader seriesReader = readersOfSelectedSeries.get(path);
            seriesDataMap.put(path, seriesReader.getNextBatchData());
            hasDataRemaining.put(path, true);
        }

        for (DynamicOneColumnData data : seriesDataMap.values()) {
            if (data.curIdx < data.timeLength) {
                heapPut(data.getTime(0));
            }
        }
    }

    @Override
    public OldRowRecord nextRowRecord() throws IOException {
        long minTime = heapGet();

        OldRowRecord record = new OldRowRecord(minTime, null, null);

        record.setTimestamp(minTime);

        for (Map.Entry<Path, DynamicOneColumnData> entry : seriesDataMap.entrySet()) {

            Path path = entry.getKey();
            DynamicOneColumnData data = entry.getValue();

            FieldV1 field = new FieldV1(data.dataType, path.getDeltaObjectToString(), path.getMeasurementToString());

            if (!hasDataRemaining.get(path)) {
                field.setNull(true);
                record.addField(field);
                continue;
            }

            int curIdx = data.curIdx;

            if (curIdx < data.timeLength) {
                if (data.getTime(curIdx) == minTime) {
                    putValueToField(data, curIdx, field);

                    data.curIdx++;
                    if (data.curIdx == data.timeLength) {
                        data = readersOfSelectedSeries.get(path).getNextBatchData();
                        seriesDataMap.put(path, data);
                        if (data.timeLength == 0) {
                            hasDataRemaining.put(path, false);
                        } else {
                            heapPut(data.getTime(data.curIdx));
                        }
                    } else {
                        heapPut(data.getTime(data.curIdx));
                    }
                } else {
                    field.setNull(true);
                }

            } else {
                field.setNull(true);
                hasDataRemaining.put(path, false);
            }

            record.addField(field);
        }

        return record;
    }

    // for the reason that heap stores duplicate elements
    protected void heapPut(long time) {
        if (!timeSet.contains(time)) {
            timeSet.add(time);
            timeHeap.add(time);
        }
    }

    protected Long heapGet() {
        Long t = timeHeap.poll();
        timeSet.remove(t);
        return t;
    }

    public void putValueToField(DynamicOneColumnData col, int idx, FieldV1 f) {
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
}

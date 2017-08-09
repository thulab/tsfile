package cn.edu.tsinghua.tsfile.timeseries.read.query;

import cn.edu.tsinghua.tsfile.timeseries.read.qp.Path;
import cn.edu.tsinghua.tsfile.timeseries.read.support.Field;
import cn.edu.tsinghua.tsfile.timeseries.read.support.RowRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.PriorityQueue;

public abstract class IteratorQueryDataSet extends QueryDataSet {
    private static final Logger logger = LoggerFactory.getLogger(IteratorQueryDataSet.class);
    public LinkedHashMap<Path, DynamicOneColumnData> retMap;
    private LinkedHashMap<Path, Boolean> hasMoreRet;

    public IteratorQueryDataSet(List<Path> paths) throws IOException {
        hasMoreRet = new LinkedHashMap<>();
        retMap = new LinkedHashMap<>();
        timeMap = new HashMap<>();
        for (Path p : paths) {
            DynamicOneColumnData res = getMoreRecordsForOneColumn(p, null);

            retMap.put(p, res);
            if (res == null || res.valueLength == 0) {
                hasMoreRet.put(p, false);
            } else {
                hasMoreRet.put(p, true);
            }
        }
    }

    public abstract DynamicOneColumnData getMoreRecordsForOneColumn(Path colName
            , DynamicOneColumnData res) throws IOException;

    public void initForRecord() {
        heap = new PriorityQueue<>(retMap.size());

        for (Path p : retMap.keySet()) {
            DynamicOneColumnData res = retMap.get(p);
            if (res != null && res.curIdx < res.valueLength) {
                heapPut(res.getTime(res.curIdx));
            }
        }
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

        if (!hasNextRecord()) {
            return null;
        }

        Long minTime = heapGet();
        RowRecord r = new RowRecord(minTime, null, null);
        for (Path p : retMap.keySet()) {
            Field f;
            DynamicOneColumnData res = retMap.get(p);
            if (res.curIdx < res.valueLength && minTime == res.getTime(res.curIdx)) {
                f = new Field(res.dataType, p.getDeltaObjectToString(), p.getMeasurementToString());
                f.setNull(false);
                putValueToField(res, res.curIdx, f);
                res.curIdx++;
                if (hasMoreRet.get(p) && res.curIdx >= res.valueLength) {
                    res.clearData();
                    try {
                        res = getMoreRecordsForOneColumn(p, res);
                    } catch (IOException e) {
                        logger.error("", e);
                    }
                    retMap.put(p, res);
                    if (res.valueLength == 0) {
                        hasMoreRet.put(p, false);
                    }
                }
                if (res.curIdx < res.valueLength) {
                    heapPut(res.getTime(res.curIdx));
                }
            } else {
                f = new Field(res.dataType, p.getMeasurementToString());
                f.setNull(true);
            }
            r.addField(f);
        }
        return r;
    }
}
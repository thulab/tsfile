package cn.edu.tsinghua.tsfile.timeseries.read.query;

import cn.edu.tsinghua.tsfile.timeseries.read.support.OldRowRecord;
import cn.edu.tsinghua.tsfile.timeseries.read.support.Path;
import cn.edu.tsinghua.tsfile.timeseries.read.support.Field;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

public abstract class IteratorOnePassQueryDataSet extends OnePassQueryDataSet {
    private static final Logger logger = LoggerFactory.getLogger(IteratorOnePassQueryDataSet.class);

    public LinkedHashMap<Path, DynamicOneColumnData> retMap;
    private LinkedHashMap<Path, Boolean> hasMoreRet;

    /**
     * init maps by data from input paths
     * @param paths
     * @throws IOException
     */
    public IteratorOnePassQueryDataSet(List<Path> paths) throws IOException {
        // init all map fields
        hasMoreRet = new LinkedHashMap<>();
        retMap = new LinkedHashMap<>();
        timeMap = new HashMap<>();

        // read data from paths
        for (Path p : paths) {
            DynamicOneColumnData res = getMoreRecordsForOneColumn(p, null);

            // update this.retMap and this.hasMoreRet
            retMap.put(p, res);
            if (res == null || res.valueLength == 0) {
                hasMoreRet.put(p, false);
            } else {
                hasMoreRet.put(p, true);
            }
        }
    }

    /**
     * read data from one path
     * will be implemented by subclasses
     *
     * @param colName
     * @param res
     * @return
     * @throws IOException
     */
    public abstract DynamicOneColumnData getMoreRecordsForOneColumn(Path colName
            , DynamicOneColumnData res) throws IOException;

    /**
     * init all data fields for querying records
     * modified by hadoop
     */
    public void initForRecord() {
        // init this.size and this.heap
        size = retMap.size();
        heap = new PriorityQueue<>(size);

        // init this.deltaObjectIds and this.measurementIds
        if (size > 0) {
            deltaObjectIds = new String[size];
            measurementIds = new String[size];
        } else {
            LOG.error("OnePassQueryDataSet init row record occurs error! the size of ret is 0.");
        }

        // update this.deltaObjectIds, this.measurementIds and time cache
        int i = 0;
        for (Path p : retMap.keySet()) {
            deltaObjectIds[i] = p.getDeltaObjectToString();
            measurementIds[i] = p.getMeasurementToString();

            DynamicOneColumnData res = retMap.get(p);
            if (res != null && res.curIdx < res.valueLength) {
                heapPut(res.getTime(res.curIdx));
            }
            i++;
        }
    }

    /**
     * judge if unread data still existed
     * @return
     */
    public boolean hasNextRecord() {
        // make sure this dataset is inited
        if (!ifInit) {
            initForRecord();
            ifInit = true;
        }
        // check if there is data in queue
        if (heap.peek() != null) {
            return true;
        }
        return false;
    }

    /**
     * get next unread record
     * modified by hadoop
     *
     * @return
     */
    public OldRowRecord getNextRecord() {
        // make sure this dataset is inited
        if (!ifInit) {
            initForRecord();
            ifInit = true;
        }

        // check if unread data exists
        if (!hasNextRecord()) {
            return null;
        }

        // get time of next record
        Long minTime = heapGet();
        // init next record
        OldRowRecord r = new OldRowRecord(minTime, null, null);
        // loop all paths
        for (Path p : retMap.keySet()) {
            Field f;
            DynamicOneColumnData res = retMap.get(p);
            // get data from res
            if (res.curIdx < res.valueLength && minTime == res.getTime(res.curIdx)) {
                f = new Field(res.dataType, p.getDeltaObjectToString(), p.getMeasurementToString());
                f.setNull(false);
                putValueToField(res, res.curIdx, f);
                res.curIdx++;
                // get rest of data
                if (hasMoreRet.get(p) && res.curIdx >= res.valueLength) {
                    res.clearData();
                    try {
                        res = getMoreRecordsForOneColumn(p, res);
                    } catch (IOException e) {
                        logger.error("", e);
                    }
                    retMap.put(p, res);
                    // update this.hasMoreRet
                    if (res.valueLength == 0) {
                        hasMoreRet.put(p, false);
                    }
                }
                // update time cache
                if (res.curIdx < res.valueLength) {
                    heapPut(res.getTime(res.curIdx));
                }
            } else {
                // construct empty record
                f = new Field(res.dataType, p.getDeltaObjectToString(), p.getMeasurementToString());
                f.setNull(true);
            }
            // put value field into record
            r.addField(f);
        }
        return r;
    }
}
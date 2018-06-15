package cn.edu.tsinghua.tsfile.timeseries.read.query;

import cn.edu.tsinghua.tsfile.common.utils.Binary;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.timeseries.read.support.Field;
import cn.edu.tsinghua.tsfile.timeseries.read.support.Path;
import cn.edu.tsinghua.tsfile.timeseries.read.support.RowRecord;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author qmm
 */
public class SegmentQueryDataSet extends QueryDataSet {
    private static final Logger logger = LoggerFactory.getLogger(SegmentQueryDataSet.class);
    private String[] keys;
    private LinkedHashMap<String, Boolean> hasMoreRet = new LinkedHashMap<>();
    private long x = 0;

    public SegmentQueryDataSet() {
        super();
    }

    public SegmentQueryDataSet(List<Path> paths) throws IOException {
        for (Path p : paths) {
            DynamicOneColumnData res = getMoreRecordsForOneColumn(p, null);

            if (res == null) {
                mapRet.put(p.getFullPath(), new DynamicOneColumnData(TSDataType.FLOAT, true, true));
            } else {
                mapRet.put(p.getFullPath(), res);
            }

            if (res == null || res.valueLength == 0) {
                hasMoreRet.put(p.getFullPath(), false);
            } else {
                hasMoreRet.put(p.getFullPath(), true);
            }
        }
    }

    public DynamicOneColumnData getMoreRecordsForOneColumn(Path colName, DynamicOneColumnData res) throws IOException {
        return null;
    }

    @Override
    public void initForRecord() {
        size = mapRet.keySet().size();

        if (size > 0) {
            keys = new String[size];
            cols = new DynamicOneColumnData[size];
            deltaObjectIds = new String[size];
            measurementIds = new String[size];
        } else {
            LOG.error("QueryDataSet init row record occurs error! the size of ret is 0.");
        }

        int i = 0;
        for (String key : mapRet.keySet()) {
            keys[i] = key;
            cols[i] = mapRet.get(key);
            cols[i].curIdx = 0;
            deltaObjectIds[i] = key.substring(0, key.lastIndexOf(PATH_SPLITTER));
            measurementIds[i] = key.substring(key.lastIndexOf(PATH_SPLITTER) + 1);
            i++;
        }
    }

    @Override
    public boolean hasNextRecord() {
        if (!ifInit) {
            initForRecord();
            ifInit = true;
        }

        for (int i = 0; i < size; ++i) {
            if (cols[i].curIdx < cols[i].timeLength) {
                return true;
            }
        }

        return false;
    }

    @Override
    public RowRecord getNextRecord() {
        if (!ifInit) {
            initForRecord();
            ifInit = true;
        }

        for (int i = 0; i < size; ++i) {
            if (cols[i].curIdx >= cols[i].timeLength) {
                continue;
            }

            Field aggregation = new Field(TSDataType.TEXT, deltaObjectIds[i], measurementIds[i]);
            aggregation.setBinaryV(Binary.valueOf(keys[i]));

            Field startTime = new Field(TSDataType.INT64, deltaObjectIds[i], measurementIds[i]);
            startTime.setLongV(cols[i].getTime(cols[i].curIdx));

            Field endTime = new Field(TSDataType.INT64, deltaObjectIds[i], measurementIds[i]);
            endTime.setLongV(cols[i].getEmptyTime(cols[i].curIdx));

            Field result = new Field(cols[i].dataType, deltaObjectIds[i], measurementIds[i]);
            putValueToField(cols[i], cols[i].curIdx, result);

            RowRecord record = new RowRecord(cols[i].getTime(cols[i].curIdx), deltaObjectIds[i], null);
            record.addField(aggregation);
            record.addField(startTime);
            record.addField(endTime);
            record.addField(result);
            cols[i].curIdx++;

            if (cols[i].curIdx >= cols[i].timeLength) {
                if (hasMoreRet.containsKey(keys[i]) && hasMoreRet.get(keys[i])) {
                    cols[i].clearData();
                    try {
                        cols[i] = getMoreRecordsForOneColumn(new Path(keys[i]), cols[i]);
                    } catch (Exception e) {
                        logger.error("", e);
                    }
                    if (cols[i].timeLength == 0) {
                        hasMoreRet.put(keys[i], false);
                    }
                }
            }

            return record;
        }

        return null;
    }
}

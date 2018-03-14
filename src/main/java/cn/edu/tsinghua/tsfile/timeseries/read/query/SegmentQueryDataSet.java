package cn.edu.tsinghua.tsfile.timeseries.read.query;

import cn.edu.tsinghua.tsfile.common.utils.Binary;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.timeseries.read.support.Field;
import cn.edu.tsinghua.tsfile.timeseries.read.support.RowRecord;
import java.util.ArrayList;
import java.util.List;

/**
 * @author qmm
 */
public class SegmentQueryDataSet extends QueryDataSet {
    private String[] keys;

    public SegmentQueryDataSet() {
        super();
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

            return record;
        }

        return null;
    }
}

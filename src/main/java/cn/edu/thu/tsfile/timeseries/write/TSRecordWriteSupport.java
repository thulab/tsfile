package cn.edu.thu.tsfile.timeseries.write;

import cn.edu.thu.tsfile.common.utils.Pair;
import cn.edu.thu.tsfile.file.metadata.enums.CompressionTypeName;
import cn.edu.thu.tsfile.timeseries.read.query.DynamicOneColumnData;
import cn.edu.thu.tsfile.timeseries.write.exception.WriteProcessException;
import cn.edu.thu.tsfile.timeseries.write.record.TSRecord;
import cn.edu.thu.tsfile.timeseries.write.series.IRowGroupWriter;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * TSRecordWriteSupport extends the super class writeSupport with
 * {@code TSRecord}. It's used to receive a TSRecord with several data points
 * and send it to responding row group.
 *
 * @author kangrong
 */
public class TSRecordWriteSupport extends WriteSupport<TSRecord> {

    private Map<String, IRowGroupWriter> groupWriters;

    @Override
    public void init(Map<String, IRowGroupWriter> groupWriters) {
        this.groupWriters = groupWriters;
    }

    @Override
    public void write(TSRecord record) throws IOException, WriteProcessException {
        String deltaObjectId = record.deltaObjectId;
        groupWriters.get(deltaObjectId).write(record.time, record.dataPointList);
    }

    @Override
    public List<Object> query(String deltaObjectId, String measurementId) {
        if (groupWriters.get(deltaObjectId) == null) {
            DynamicOneColumnData left = null;
            Pair<List<ByteArrayInputStream>, CompressionTypeName> right = null;
            List<Object> result = new ArrayList<>();
            result.add(left);
            result.add(right);
            return result;
        }
        return groupWriters.get(deltaObjectId).query(measurementId);
    }
}

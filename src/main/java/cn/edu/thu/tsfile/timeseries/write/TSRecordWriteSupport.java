package cn.edu.thu.tsfile.timeseries.write;

import java.io.IOException;
import java.util.Map;

import cn.edu.thu.tsfile.timeseries.write.record.TSRecord;
import cn.edu.thu.tsfile.timeseries.write.exception.WriteProcessException;
import cn.edu.thu.tsfile.timeseries.write.series.IRowGroupWriter;

/**
 * TSRecordWriteSupport extends the super class {@coderiteSupport} with {@code TSRecord}. It's used
 * to receive a TSRecord with several data points and send it to responding row group.
 * 
 * @see TSRecord TSRecord
 * @author kangrong
 *
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
}

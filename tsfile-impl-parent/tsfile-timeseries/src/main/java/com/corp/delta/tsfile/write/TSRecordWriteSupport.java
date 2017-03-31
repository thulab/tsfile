package com.corp.delta.tsfile.write;

import java.io.IOException;
import java.util.Map;

import com.corp.delta.tsfile.write.exception.WriteProcessException;
import com.corp.delta.tsfile.write.record.TSRecord;
import com.corp.delta.tsfile.write.series.IRowGroupWriter;

/**
 * TSRecordWriteSupport extends the super class {@coderiteSupport} with {@code TSRecord}. It's used
 * to receive a TSRecord with several data points and send it to responding row group.
 * 
 * @see com.corp.delta.tsfile.write.record.TSRecord TSRecord
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

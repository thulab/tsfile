package cn.edu.thu.tsfile.timeseries.write;

import java.io.IOException;

import cn.edu.thu.tsfile.common.conf.TSFileConfig;
import cn.edu.thu.tsfile.timeseries.write.io.TSFileIOWriter;
import cn.edu.thu.tsfile.timeseries.write.record.TSRecord;
import cn.edu.thu.tsfile.timeseries.write.schema.FileSchema;

/**
 * TSRecordWriter extends InternalRecordWriter and overrides method {@code checkRowGroup()} and
 * {@code writeError()}
 * 
 * @see TSRecord TSRecord
 * @see InternalRecordWriter InternalRecordWriter
 * @author kangrong
 */
public class TSRecordWriter extends InternalRecordWriter<TSRecord> {
    public TSRecordWriter(TSFileConfig conf, TSFileIOWriter tsfileIOWriter,
                          WriteSupport<TSRecord> writeSupport, FileSchema schema) {
        super(conf, tsfileIOWriter, writeSupport, schema);
    }

    @Override
    protected boolean checkRowGroup(TSRecord record) throws IOException {
        if (!schema.hasDeltaObject(record.deltaObjectId)) {
            schema.addDeltaObject(record.deltaObjectId);
            addGroupToInternalRecordWriter(record.deltaObjectId);
        }
        return true;
    }
}

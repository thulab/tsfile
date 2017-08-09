package cn.edu.tsinghua.tsfile.timeseries.write;

import cn.edu.tsinghua.tsfile.common.conf.TSFileConfig;
import cn.edu.tsinghua.tsfile.timeseries.write.io.TSFileIOWriter;
import cn.edu.tsinghua.tsfile.timeseries.write.record.TSRecord;
import cn.edu.tsinghua.tsfile.timeseries.write.schema.FileSchema;

import java.io.IOException;

/**
 * TSRecordWriter extends InternalRecordWriter and overrides method {@code checkRowGroup()} and
 * {@code writeError()}
 *
 * @author kangrong
 * @see TSRecord TSRecord
 * @see InternalRecordWriter InternalRecordWriter
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

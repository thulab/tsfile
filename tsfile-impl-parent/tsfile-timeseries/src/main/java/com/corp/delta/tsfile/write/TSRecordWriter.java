package com.corp.delta.tsfile.write;

import com.corp.delta.tsfile.common.conf.TSFileConfig;
import com.corp.delta.tsfile.write.io.TSFileIOWriter;
import com.corp.delta.tsfile.write.record.TSRecord;
import com.corp.delta.tsfile.write.schema.FileSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * TSRecordWriter extends InternalRecordWriter and overrides method {@code checkRowGroup()} and
 * {@code writeError()}
 * 
 * @see com.corp.delta.tsfile.write.record.TSRecord TSRecord
 * @see com.corp.delta.tsfile.write.InternalRecordWriter InternalRecordWriter
 * @author kangrong
 */
public class TSRecordWriter extends InternalRecordWriter<TSRecord> {
    private static Logger LOG = LoggerFactory.getLogger(TSRecordWriter.class);

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

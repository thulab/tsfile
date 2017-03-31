package com.corp.delta.tsfile.write.record.datapoint;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.corp.delta.tsfile.file.metadata.enums.TSDataType;
import com.corp.delta.tsfile.write.record.DataPoint;
import com.corp.delta.tsfile.write.series.ISeriesWriter;

/**
 * a subclass for Enum data type extends DataPoint
 * 
 * @see com.corp.delta.tsfile.write.record.DataPoint DataPoint
 * @author kangrong
 *
 */
public class EnumDataPoint extends DataPoint {
    private static final Logger LOG = LoggerFactory.getLogger(EnumDataPoint.class);
    private int value;

    public EnumDataPoint(String measurementId, int v) {
        super(TSDataType.ENUMS, measurementId);
        this.value = v;
    }

    @Override
    public void write(long time, ISeriesWriter writer) throws IOException {
        if (writer == null) {
            LOG.warn("given ISeriesWriter is null, do nothing and return");
            return;
        }
        writer.write(time, value);

    }

    @Override
    public Object getValue() {
        return value;
    }

    @Override
    public void setInteger(int value) {
        this.value = value;
    }
}

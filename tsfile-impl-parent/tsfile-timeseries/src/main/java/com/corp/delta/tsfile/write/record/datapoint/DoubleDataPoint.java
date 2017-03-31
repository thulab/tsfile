package com.corp.delta.tsfile.write.record.datapoint;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.corp.delta.tsfile.file.metadata.enums.TSDataType;
import com.corp.delta.tsfile.write.record.DataPoint;
import com.corp.delta.tsfile.write.series.ISeriesWriter;

/**
 * a subclass for Double data type extends DataPoint
 * 
 * @see com.corp.delta.tsfile.write.record.DataPoint DataPoint
 * @author kangrong
 *
 */
public class DoubleDataPoint extends DataPoint {
    private static final Logger LOG = LoggerFactory.getLogger(DoubleDataPoint.class);
    private double value;

    public DoubleDataPoint(String measurementId, double v) {
        super(TSDataType.DOUBLE, measurementId);
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
    public void setDouble(double value) {
        this.value = value;
    }
}

package cn.edu.thu.tsfile.timeseries.write.record.datapoint;

import java.io.IOException;

import cn.edu.thu.tsfile.timeseries.write.series.ISeriesWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.thu.tsfile.file.metadata.enums.TSDataType;
import cn.edu.thu.tsfile.timeseries.write.record.DataPoint;

/**
 * a subclass for Double data type extends DataPoint
 * 
 * @see DataPoint DataPoint
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

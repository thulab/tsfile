package cn.edu.thu.tsfile.timeseries.write.record.datapoint;

import java.io.IOException;

import cn.edu.thu.tsfile.timeseries.write.record.DataPoint;
import cn.edu.thu.tsfile.timeseries.write.series.ISeriesWriter;
import cn.edu.thu.tsfile.file.metadata.enums.TSDataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * a subclass for Boolean data type extends DataPoint
 * 
 * @see DataPoint DataPoint
 * @author kangrong
 *
 */
public class BooleanDataPoint extends DataPoint {
    private static final Logger LOG = LoggerFactory.getLogger(BooleanDataPoint.class);
    private boolean value;

    public BooleanDataPoint(String measurementId, boolean v) {
        super(TSDataType.BOOLEAN, measurementId);
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
    public void setBoolean(boolean value) {
        this.value = value;
    }
}

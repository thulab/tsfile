package cn.edu.thu.tsfile.timeseries.write.record.datapoint;

import java.io.IOException;

import cn.edu.thu.tsfile.file.metadata.enums.TSDataType;
import cn.edu.thu.tsfile.timeseries.write.record.DataPoint;
import cn.edu.thu.tsfile.timeseries.write.series.ISeriesWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * a subclass for Float data type extends DataPoint
 * 
 * @see DataPoint DataPoint
 * @author kangrong
 *
 */
public class FloatDataPoint extends DataPoint {
    private static final Logger LOG = LoggerFactory.getLogger(FloatDataPoint.class);
    private float value;

    public FloatDataPoint(String measurementId, float v) {
        super(TSDataType.FLOAT, measurementId);
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
    public void setFloat(float value) {
        this.value = value;
    }
}

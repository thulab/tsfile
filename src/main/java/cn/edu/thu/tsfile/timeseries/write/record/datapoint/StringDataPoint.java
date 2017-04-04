package cn.edu.thu.tsfile.timeseries.write.record.datapoint;

import cn.edu.thu.tsfile.common.utils.Binary;
import cn.edu.thu.tsfile.file.metadata.enums.TSDataType;
import cn.edu.thu.tsfile.timeseries.write.record.DataPoint;
import cn.edu.thu.tsfile.timeseries.write.series.ISeriesWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * a subclass for Integer data type extends DataPoint
 * 
 * @see DataPoint DataPoint
 * @author kangrong
 *
 */
public class StringDataPoint extends DataPoint {
    private static final Logger LOG = LoggerFactory.getLogger(StringDataPoint.class);
    private Binary value;

    public StringDataPoint(String measurementId, Binary v) {
        super(TSDataType.BYTE_ARRAY, measurementId);
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
    public void setString(Binary value) {
        this.value = value;
    }
}

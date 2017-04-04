package cn.edu.thu.tsfile.timeseries.write.record.datapoint;

import java.io.IOException;
import java.math.BigDecimal;

import cn.edu.thu.tsfile.timeseries.write.series.ISeriesWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.thu.tsfile.file.metadata.enums.TSDataType;
import cn.edu.thu.tsfile.timeseries.write.record.DataPoint;

/**
 * a subclass for BigDecimal data type extends DataPoint
 * 
 * @see DataPoint DataPoint
 * @author kangrong
 *
 */
public class BigDecimalDataPoint extends DataPoint {
    private static final Logger LOG = LoggerFactory.getLogger(BigDecimalDataPoint.class);
    private BigDecimal value;

    public BigDecimalDataPoint(String measurementId, BigDecimal v) {
        super(TSDataType.BIGDECIMAL, measurementId);
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
    public void setBigDecimal(BigDecimal value) {
        this.value = value;
    }

}

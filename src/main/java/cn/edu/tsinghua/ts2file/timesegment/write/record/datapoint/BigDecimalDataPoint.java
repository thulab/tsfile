package cn.edu.tsinghua.ts2file.timesegment.write.record.datapoint;

import cn.edu.tsinghua.ts2file.timesegment.write.record.SegmentDataPoint;
import cn.edu.tsinghua.ts2file.timesegment.write.segment.ISegmentWriter;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import java.io.IOException;
import java.math.BigDecimal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class BigDecimalDataPoint extends SegmentDataPoint {
    private static final Logger LOG = LoggerFactory.getLogger(BigDecimalDataPoint.class);
    private BigDecimal value;

    public BigDecimalDataPoint(String measurementId, BigDecimal v) {
        super(TSDataType.BIGDECIMAL, measurementId);
        this.value = v;
    }

    @Override
    public void write(long startTime, long endTime, ISegmentWriter writer) throws IOException {
        if (writer == null) {
            LOG.warn("given IWindowWriter is null, do nothing and return");
            return;
        }
        writer.write(startTime, endTime, value);
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

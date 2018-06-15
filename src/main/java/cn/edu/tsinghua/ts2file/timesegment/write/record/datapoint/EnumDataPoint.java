package cn.edu.tsinghua.ts2file.timesegment.write.record.datapoint;

import cn.edu.tsinghua.ts2file.timesegment.write.record.SegmentDataPoint;
import cn.edu.tsinghua.ts2file.timesegment.write.segment.ISegmentWriter;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class EnumDataPoint extends SegmentDataPoint {
    private static final Logger LOG = LoggerFactory.getLogger(EnumDataPoint.class);
    private int value;

    public EnumDataPoint(String measurementId, int v) {
        super(TSDataType.ENUMS, measurementId);
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
    public void setInteger(int value) {
        this.value = value;
    }
}

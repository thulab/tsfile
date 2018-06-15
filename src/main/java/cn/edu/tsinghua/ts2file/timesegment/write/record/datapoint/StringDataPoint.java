package cn.edu.tsinghua.ts2file.timesegment.write.record.datapoint;

import cn.edu.tsinghua.ts2file.timesegment.write.segment.ISegmentWriter;
import cn.edu.tsinghua.tsfile.common.utils.Binary;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.ts2file.timesegment.write.record.SegmentDataPoint;
import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class StringDataPoint extends SegmentDataPoint {
    private static final Logger LOG = LoggerFactory.getLogger(StringDataPoint.class);
    private Binary value;

    public StringDataPoint(String measurementId, Binary v) {
        super(TSDataType.TEXT, measurementId);
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
    public void setString(Binary value) {
        this.value = value;
    }
}

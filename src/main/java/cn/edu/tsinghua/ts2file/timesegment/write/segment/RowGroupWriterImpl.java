package cn.edu.tsinghua.ts2file.timesegment.write.segment;

import cn.edu.tsinghua.tsfile.timeseries.write.desc.MeasurementDescriptor;
import cn.edu.tsinghua.tsfile.timeseries.write.exception.NoMeasurementException;
import cn.edu.tsinghua.tsfile.timeseries.write.exception.WriteProcessException;
import cn.edu.tsinghua.tsfile.timeseries.write.io.TsFileIOWriter;
import cn.edu.tsinghua.tsfile.timeseries.write.page.IPageWriter;
import cn.edu.tsinghua.tsfile.timeseries.write.page.PageWriterImpl;
import cn.edu.tsinghua.ts2file.timesegment.write.record.SegmentDataPoint;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by qmm on 18/3/14.
 */
public class RowGroupWriterImpl implements IRowGroupWriter {

  private static Logger LOG = LoggerFactory.getLogger(RowGroupWriterImpl.class);
  private final String deltaObjectId;
  private Map<String, ISegmentWriter> segmentWriters = new HashMap<>();

  public RowGroupWriterImpl(String deltaObjectId) {
    this.deltaObjectId = deltaObjectId;
  }

  @Override
  public void write(long startTime, long endTime, List<SegmentDataPoint> data)
      throws WriteProcessException, IOException {
    for (SegmentDataPoint point : data) {
      String measurementId = point.getMeasurementId();
      if (!segmentWriters.containsKey(measurementId)) {
        throw new NoMeasurementException("measurement id " + measurementId + " not found!");
      }

      point.write(startTime, endTime, segmentWriters.get(measurementId));
    }
  }

  @Override
  public void flushToFileWriter(TsFileIOWriter deltaFileWriter) throws IOException {
    LOG.debug("start flush delta object id:{}", deltaObjectId);
    for (ISegmentWriter windowWriter : segmentWriters.values()) {
      windowWriter.writeToFileWriter(deltaFileWriter);
    }
  }

  @Override
  public long updateMaxGroupMemSize() {
    long bufferSize = 0;
    for (ISegmentWriter windowWriter : segmentWriters.values())
      bufferSize += windowWriter.estimateMaxWindowMemSize();
    return bufferSize;
  }

  @Override
  public void addWindowWriter(MeasurementDescriptor desc, int pageSize) {
    if(!segmentWriters.containsKey(desc.getMeasurementId())) {
      IPageWriter pageWriter = new PageWriterImpl(desc);
      ISegmentWriter windowWriter = new SegmentWriterImpl(deltaObjectId, desc, pageWriter, pageSize);
      this.segmentWriters.put(desc.getMeasurementId(), windowWriter);
    }
  }
}

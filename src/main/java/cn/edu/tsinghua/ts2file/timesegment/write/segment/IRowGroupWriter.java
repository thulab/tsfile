package cn.edu.tsinghua.ts2file.timesegment.write.segment;

import cn.edu.tsinghua.ts2file.timesegment.write.record.SegmentDataPoint;
import cn.edu.tsinghua.tsfile.timeseries.write.desc.MeasurementDescriptor;
import cn.edu.tsinghua.tsfile.timeseries.write.exception.WriteProcessException;
import cn.edu.tsinghua.tsfile.timeseries.write.io.TsFileIOWriter;
import java.io.IOException;
import java.util.List;

/**
 * Created by qmm on 18/3/14.
 */
public interface IRowGroupWriter {
  /**
   * receive a timestamp and a list of data points, write them to themselves
   * series writers.
   *
   * @param startTime - window start time
   * @param endTime - window end time
   * @param data - data point list to input
   * @throws WriteProcessException exception in write process
   * @throws IOException exception in IO
   */
  void write(long startTime, long endTime, List<SegmentDataPoint> data) throws WriteProcessException, IOException;

  /**
   * flushing method for outputting to OS file system or HDFS.
   *
   * @param tsfileWriter - TSFileIOWriter
   * @throws IOException exception in IO
   */
  void flushToFileWriter(TsFileIOWriter tsfileWriter) throws IOException;

  /**
   * Note that, this method should be called after running
   * {@code long calcAllocatedSize()}
   *
   * @return - allocated memory size.
   */
  long updateMaxGroupMemSize();

  /**
   * given a measurement descriptor, create a corresponding writer and put into this RowGroupWriter
   *
   * @param measurementDescriptor a measurement descriptor containing the message of the series
   * @param pageSize the specified page size
   */
  void addWindowWriter(MeasurementDescriptor measurementDescriptor, int pageSize);
}

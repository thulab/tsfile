package cn.edu.tsinghua.ts2file.timesegment.write.segment;

import cn.edu.tsinghua.tsfile.common.utils.Binary;
import cn.edu.tsinghua.tsfile.timeseries.write.io.TsFileIOWriter;
import java.io.IOException;
import java.math.BigDecimal;

/**
 * Created by qmm on 18/3/14.
 */
public interface ISegmentWriter {
  void write(long startTime, long endTime, int value) throws IOException;

  void write(long startTime, long endTime, long value) throws IOException;

  void write(long startTime, long endTime, boolean value) throws IOException;

  void write(long startTime, long endTime, float value) throws IOException;

  void write(long startTime, long endTime, double value) throws IOException;

  void write(long startTime, long endTime, BigDecimal value) throws IOException;

  void write(long startTime, long endTime, Binary value) throws IOException;

  void writeToFileWriter(TsFileIOWriter tsfileWriter) throws IOException;

  long estimateMaxWindowMemSize();
}

package cn.edu.tsinghua.ts2file.timesegment.write.page;

import cn.edu.tsinghua.tsfile.common.utils.ListByteArrayOutputStream;
import cn.edu.tsinghua.tsfile.timeseries.write.exception.PageException;
import cn.edu.tsinghua.tsfile.timeseries.write.io.TsFileIOWriter;
import java.io.IOException;

/**
 * Created by qmm on 18/3/14.
 */
public interface IPageWriter {
  /**
   * store a page to this pageWriter.
   *
   * @param listByteArray - the data to be stored to pageWriter
   * @param valueCount    - the amount of values in that page
   * @param maxTimestamp  - timestamp maximum in given data
   * @param minTimestamp  - timestamp minimum in given data
   * @throws PageException - if an PageException occurs.
   */
  void writePage(ListByteArrayOutputStream listByteArray, int valueCount,
      long maxTimestamp, long minTimestamp) throws PageException;

  /**
   * write the page to specified IOWriter
   *
   * @param writer the specified IOWriter
   * @throws IOException exception in IO
   */
  void writeToFileWriter(TsFileIOWriter writer) throws IOException;

  /**
   * reset exist data in page for next stage
   */
  void reset();

  /**
   * @return the max possible allocated size
   */
  long estimateMaxPageMemSize();
}

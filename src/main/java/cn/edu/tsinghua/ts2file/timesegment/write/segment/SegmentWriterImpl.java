package cn.edu.tsinghua.ts2file.timesegment.write.segment;

import cn.edu.tsinghua.tsfile.common.conf.TSFileDescriptor;
import cn.edu.tsinghua.tsfile.common.utils.Binary;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.file.metadata.statistics.Statistics;
import cn.edu.tsinghua.tsfile.timeseries.write.desc.MeasurementDescriptor;
import cn.edu.tsinghua.tsfile.timeseries.write.exception.PageException;
import cn.edu.tsinghua.tsfile.timeseries.write.io.TsFileIOWriter;
import cn.edu.tsinghua.tsfile.timeseries.write.page.IPageWriter;
import java.io.IOException;
import java.math.BigDecimal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by qmm on 18/3/14.
 */
public class SegmentWriterImpl implements ISegmentWriter {

  private static final Logger LOG = LoggerFactory.getLogger(SegmentWriterImpl.class);
  private static final int MINIMUM_RECORD_COUNT_FOR_CHECK = 1;

  private final TSDataType dataType;
  private final IPageWriter pageWriter;
  /**
   * page size threshold
   */
  private final long psThres;
  private final int pageCountUpperBound;
  /**
   * value writer to encode data
   */
  private ValueWriter dataValueWriter;

  /**
   * value count on of a page. It will be reset after calling
   * {@code writePage()}
   */
  private int valueCount;
  private int valueCountForNextSizeCheck;
  /**
   * statistic on a page. It will be reset after calling {@code writePage()}
   */
  private Statistics<?> pageStatistics;
  /**
   * statistic on a stage. It will be reset after calling
   * {@code writeToFileWriter()}
   */
  private Statistics<?> windowStatistics;
  private long time;
  private long minTimestamp = -1;
  private String deltaObjectId;
  private MeasurementDescriptor desc;

  public SegmentWriterImpl(String deltaObjectId, MeasurementDescriptor desc, IPageWriter pageWriter,
      int pageSizeThreshold) {
    this.deltaObjectId = deltaObjectId;
    this.desc = desc;
    this.dataType = desc.getType();
    this.pageWriter = pageWriter;
    this.psThres = pageSizeThreshold;
    // initial check of memory usage. So that we have enough data to make an
    // initial prediction
    this.valueCountForNextSizeCheck = MINIMUM_RECORD_COUNT_FOR_CHECK;
    this.windowStatistics = Statistics.getStatsByType(desc.getType());
    resetPageStatistics();
    this.dataValueWriter = new ValueWriter();
    this.pageCountUpperBound = TSFileDescriptor.getInstance().getConfig().maxNumberOfPointsInPage;

    this.dataValueWriter.setStartTimeEncoder(desc.getTimeEncoder());
    this.dataValueWriter.setEndTimeEncoder(desc.getTimeEncoder());
    this.dataValueWriter.setValueEncoder(desc.getValueEncoder());
  }

  private void resetPageStatistics() {
    this.pageStatistics = Statistics.getStatsByType(dataType);
  }

  /**
   * check occupied memory size, if it exceeds the PageSize threshold, flush
   * them to given OutputStream.
   */
  private void checkPageSize() {
    if (valueCount == pageCountUpperBound) {
      LOG.debug("current line count reaches the upper bound, write page {}", desc);
      writePage();
    } else if (valueCount >= valueCountForNextSizeCheck) {
      // not checking the memory used for every value
      long currentColumnSize = dataValueWriter.estimateMaxMemSize();
      if (currentColumnSize > psThres) {
        // we will write the current page
        LOG.debug("enough size, write page {}", desc);
        writePage();
      } else {
        LOG.debug("{}:{} not enough size, now: {}, change to {}", deltaObjectId, desc, valueCount,
            valueCountForNextSizeCheck);
      }
      // reset the valueCountForNextSizeCheck for the next page
      valueCountForNextSizeCheck = (int) (((float) psThres / currentColumnSize) * valueCount);
    }
  }

  /**
   * pack data into {@code IPageWriter}
   */
  private void writePage() {
    try {
      pageWriter.writePage(dataValueWriter.getBytes(), valueCount, pageStatistics, time, minTimestamp);
      this.windowStatistics.mergeStatistics(this.pageStatistics);
    } catch (IOException e) {
      LOG.error("meet error in dataValueWriter.getBytes(),ignore this page, {}", e.getMessage());
    } catch (PageException e) {
      LOG.error("meet error in pageWriter.writePage,ignore this page, error message:{}", e.getMessage());
    } finally {
      // clear start time stamp for next initializing
      minTimestamp = -1;
      valueCount = 0;
      dataValueWriter.reset();
      resetPageStatistics();
    }
  }

  @Override
  public void write(long startTime, long endTime, int value) throws IOException {
    this.time = endTime;
    ++valueCount;
    dataValueWriter.write(startTime, endTime, value);
    pageStatistics.updateStats(value);
    if (minTimestamp == -1)
      minTimestamp = startTime;
    checkPageSize();
  }

  @Override
  public void write(long startTime, long endTime, long value) throws IOException {
    this.time = endTime;
    ++valueCount;
    dataValueWriter.write(startTime, endTime, value);
    pageStatistics.updateStats(value);
    if (minTimestamp == -1)
      minTimestamp = startTime;
    checkPageSize();
  }

  @Override
  public void write(long startTime, long endTime, boolean value) throws IOException {
    this.time = endTime;
    ++valueCount;
    dataValueWriter.write(startTime, endTime, value);
    pageStatistics.updateStats(value);
    if (minTimestamp == -1)
      minTimestamp = startTime;
    checkPageSize();
  }

  @Override
  public void write(long startTime, long endTime, float value) throws IOException {
    this.time = endTime;
    ++valueCount;
    dataValueWriter.write(startTime, endTime, value);
    pageStatistics.updateStats(value);
    if (minTimestamp == -1)
      minTimestamp = startTime;
    checkPageSize();
  }

  @Override
  public void write(long startTime, long endTime, double value) throws IOException {
    this.time = endTime;
    ++valueCount;
    dataValueWriter.write(startTime, endTime, value);
    pageStatistics.updateStats(value);
    if (minTimestamp == -1)
      minTimestamp = startTime;
    checkPageSize();
  }

  @Override
  public void write(long startTime, long endTime, BigDecimal value) throws IOException {
    this.time = endTime;
    ++valueCount;
    dataValueWriter.write(startTime, endTime, value);
    pageStatistics.updateStats(value);
    if (minTimestamp == -1)
      minTimestamp = startTime;
    checkPageSize();
  }

  @Override
  public void write(long startTime, long endTime, Binary value) throws IOException {
    this.time = endTime;
    ++valueCount;
    dataValueWriter.write(startTime, endTime, value);
    pageStatistics.updateStats(value);
    if (minTimestamp == -1)
      minTimestamp = startTime;
    checkPageSize();
  }

  @Override
  public void writeToFileWriter(TsFileIOWriter tsfileWriter) throws IOException {
    if (valueCount > 0) {
      writePage();
    }
    pageWriter.writeToFileWriter(tsfileWriter, windowStatistics);
    pageWriter.reset();
    // reset series_statistics
    this.windowStatistics = Statistics.getStatsByType(dataType);
  }

  @Override
  public long estimateMaxWindowMemSize() {
    return dataValueWriter.estimateMaxMemSize() + pageWriter.estimateMaxPageMemSize();
  }
}

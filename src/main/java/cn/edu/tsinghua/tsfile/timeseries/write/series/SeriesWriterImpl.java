package cn.edu.tsinghua.tsfile.timeseries.write.series;

import java.io.IOException;
import java.math.BigDecimal;

import cn.edu.tsinghua.tsfile.file.header.ChunkHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.tsinghua.tsfile.common.conf.TSFileDescriptor;
import cn.edu.tsinghua.tsfile.common.utils.Binary;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.file.metadata.statistics.Statistics;
import cn.edu.tsinghua.tsfile.timeseries.write.desc.MeasurementDescriptor;
import cn.edu.tsinghua.tsfile.timeseries.write.exception.PageException;
import cn.edu.tsinghua.tsfile.timeseries.write.io.TsFileIOWriter;
import cn.edu.tsinghua.tsfile.timeseries.write.page.IPageWriter;

/**
 * //TODO 这个类目前看 跟pageWriter很可能可以合并。。。
 *
 * A implementation of {@code ISeriesWriter}. {@code SeriesWriterImpl} consists
 * of a {@code PageWriter}, a {@code ValueWriter}, and two {@code Statistics}.
 *
 * @author kangrong
 * @see ISeriesWriter ISeriesWriter
 */
public class SeriesWriterImpl implements ISeriesWriter {
    private static final Logger LOG = LoggerFactory.getLogger(SeriesWriterImpl.class);
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
     * {@code writePageHeaderAndDataIntoBuff()}
     */
    private int valueCount;
    private int valueCountForNextSizeCheck;
    /**
     * statistic on a page. It will be reset after calling {@code writePageHeaderAndDataIntoBuff()}
     */
    private Statistics<?> pageStatistics;
    /**
     * statistic on a stage. It will be reset after calling
     * {@code writeAllPagesOfSeriesToTsFile()}
     */
    private Statistics<?> seriesStatistics;
    private long time;
    private long minTimestamp = -1;
    private String deltaObjectId;//TODO 这个没用吧。。

    private MeasurementDescriptor desc;


    private int numOfPages;

    public SeriesWriterImpl(String deltaObjectId, MeasurementDescriptor desc, IPageWriter pageWriter,
                            int pageSizeThreshold) {
        this.deltaObjectId = deltaObjectId;
        this.desc = desc;
        this.dataType = desc.getType();
        this.pageWriter = pageWriter;
        this.psThres = pageSizeThreshold;
        // initial check of memory usage. So that we have enough data to make an
        // initial prediction
        this.valueCountForNextSizeCheck = MINIMUM_RECORD_COUNT_FOR_CHECK;
        this.seriesStatistics = Statistics.getStatsByType(desc.getType());
        resetPageStatistics();
        this.dataValueWriter = new ValueWriter();
        this.pageCountUpperBound = TSFileDescriptor.getInstance().getConfig().maxNumberOfPointsInPage;

        this.dataValueWriter.setTimeEncoder(desc.getTimeEncoder());
        this.dataValueWriter.setValueEncoder(desc.getValueEncoder());
    }

    private void resetPageStatistics() {
        this.pageStatistics = Statistics.getStatsByType(dataType);
    }

    @Override
    public void write(long time, long value) throws IOException {
        this.time = time;
        ++valueCount;
        dataValueWriter.write(time, value);
        pageStatistics.updateStats(value);
        if (minTimestamp == -1)
            minTimestamp = time;
        checkPageSizeAndMayOpenANewPage();
    }

    @Override
    public void write(long time, int value) throws IOException {
        this.time = time;
        ++valueCount;
        dataValueWriter.write(time, value);
        pageStatistics.updateStats(value);
        if (minTimestamp == -1)
            minTimestamp = time;
        checkPageSizeAndMayOpenANewPage();
    }

    @Override
    public void write(long time, boolean value) throws IOException {
        this.time = time;
        ++valueCount;
        dataValueWriter.write(time, value);
        pageStatistics.updateStats(value);
        if (minTimestamp == -1)
            minTimestamp = time;
        checkPageSizeAndMayOpenANewPage();
    }

    @Override
    public void write(long time, float value) throws IOException {
        this.time = time;
        ++valueCount;
        dataValueWriter.write(time, value);
        pageStatistics.updateStats(value);
        if (minTimestamp == -1)
            minTimestamp = time;
        checkPageSizeAndMayOpenANewPage();
    }

    @Override
    public void write(long time, double value) throws IOException {
        this.time = time;
        ++valueCount;
        dataValueWriter.write(time, value);
        pageStatistics.updateStats(value);
        if (minTimestamp == -1)
            minTimestamp = time;
        checkPageSizeAndMayOpenANewPage();
    }

    @Override
    public void write(long time, BigDecimal value) throws IOException {
        this.time = time;
        ++valueCount;
        dataValueWriter.write(time, value);
        pageStatistics.updateStats(value);
        if (minTimestamp == -1)
            minTimestamp = time;
        checkPageSizeAndMayOpenANewPage();
    }

    @Override
    public void write(long time, Binary value) throws IOException {
        this.time = time;
        ++valueCount;
        dataValueWriter.write(time, value);
        pageStatistics.updateStats(value);
        if (minTimestamp == -1)
            minTimestamp = time;
        checkPageSizeAndMayOpenANewPage();
    }

    /**
     * check occupied memory size, if it exceeds the PageSize threshold, flush
     * them to given OutputStream.
     */
    private void checkPageSizeAndMayOpenANewPage() {
        if (valueCount == pageCountUpperBound) {
            LOG.debug("current line count reaches the upper bound, writeTo page {}", desc);
            writePage();
        } else if (valueCount >= valueCountForNextSizeCheck) {
            // not checking the memory used for every value
            long currentColumnSize = dataValueWriter.estimateMaxMemSize();
            if (currentColumnSize > psThres) {
                // we will writeTo the current page
                LOG.debug("enough size, writeTo page {}", desc);
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
            pageWriter.writePageHeaderAndDataIntoBuff(dataValueWriter.getBytes(), valueCount, pageStatistics, time, minTimestamp);
            this.seriesStatistics.mergeStatistics(this.pageStatistics);
            numOfPages++;
        } catch (IOException e) {
            LOG.error("meet error in dataValueWriter.getBytes(),ignore this page, {}", e.getMessage());
        } catch (PageException e) {
            LOG.error("meet error in pageWriter.writePageHeaderAndDataIntoBuff,ignore this page, error message:{}", e.getMessage());
        } finally {
            // clear start time stamp for next initializing
            minTimestamp = -1;
            valueCount = 0;
            dataValueWriter.reset();
            resetPageStatistics();
        }
    }

    @Override
    public void writeToFileWriter(TsFileIOWriter tsfileWriter) throws IOException {

        pageWriter.writeAllPagesOfSeriesToTsFile(tsfileWriter, seriesStatistics, numOfPages);
        pageWriter.reset();
        // reset series_statistics
        this.seriesStatistics = Statistics.getStatsByType(dataType);
    }

    @Override
    public long estimateMaxSeriesMemSize() {
        return dataValueWriter.estimateMaxMemSize() + pageWriter.estimateMaxPageMemSize();
    }


    //return the serialized size of the chunk header + all pages
    @Override
    public long getCurrentChunkSize(){
        return ChunkHeader.getSerializedSize(desc.getMeasurementId()) + pageWriter.getCurrentDataSize();
    }

    @Override
    public void preFlush() {
        if (valueCount > 0) {
            writePage();
        }
    }
    @Override
    public int getNumOfPages() {
        return numOfPages;
    }
}

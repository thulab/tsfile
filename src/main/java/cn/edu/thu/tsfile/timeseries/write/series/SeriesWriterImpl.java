package cn.edu.thu.tsfile.timeseries.write.series;

import java.io.IOException;
import java.math.BigDecimal;

import cn.edu.thu.tsfile.common.conf.TSFileConfig;
import cn.edu.thu.tsfile.common.conf.TSFileDescriptor;
import cn.edu.thu.tsfile.common.utils.Binary;
import cn.edu.thu.tsfile.timeseries.write.desc.MeasurementDescriptor;
import cn.edu.thu.tsfile.encoding.encoder.Encoder;
import cn.edu.thu.tsfile.file.metadata.enums.TSDataType;
import cn.edu.thu.tsfile.file.metadata.statistics.Statistics;
import cn.edu.thu.tsfile.timeseries.write.exception.PageException;
import cn.edu.thu.tsfile.timeseries.write.io.TSFileIOWriter;
import cn.edu.thu.tsfile.timeseries.write.page.IPageWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A implementation of {@code ISeriesWriter}. {@code SeriesWriterImpl} consists of a
 * {@code PageWriter}, a {@code ValueWriter}, and two {@code Statistics}.
 * 
 * @see ISeriesWriter ISeriesWriter
 * @author kangrong
 *
 */
public class SeriesWriterImpl implements ISeriesWriter {
    private static final Logger LOG = LoggerFactory.getLogger(SeriesWriterImpl.class);

    private final TSDataType dataType;
    private final IPageWriter pageWriter;
    /**
     * page size threshold
     */
    private final long psThres;
    /**
     * value writer to encode data
     */
    private ValueWriter dataValueWriter;
    /**
     * value count on of a page. It will be reset after calling {@code writePage()}
     */
    private int valueCount;
    private int valueCountForNextSizeCheck;
    /**
     * statistic on a page. It will be reset after calling {@code writePage()}
     */
    private Statistics<?> pageStatistics;
    /**
     * statistic on a stage. It will be reset after calling {@code writeToFileWriter()}
     */
    private Statistics<?> seriesStatistics;
    private long time;
    private long minTimestamp = -1;
    private String deltaObjectId;
    private MeasurementDescriptor desc;

    public SeriesWriterImpl(String deltaObjectId, MeasurementDescriptor desc,
            IPageWriter pageWriter, int pageSizeThreshold) {
        TSFileConfig conf = TSFileDescriptor.getInstance().getConfig();
        this.deltaObjectId = deltaObjectId;
        this.desc = desc;
        this.dataType = desc.getType();
        this.pageWriter = pageWriter;
        this.psThres = pageSizeThreshold;
        // initial check of memory usage. So that we have enough data to make an
        // initial prediction
        this.valueCountForNextSizeCheck = conf.pageCheckSizeThreshold;
        this.seriesStatistics = Statistics.getStatsByType(desc.getType());
        resetPageStatistics();
        this.dataValueWriter = new ValueWriter();

        this.dataValueWriter.setTimeEncoder(desc.getTimeEncoder());
        this.dataValueWriter.setValueEncoder(desc.getValueEncoder());
        // initialize frequency
        Encoder freqEncoder = desc.getFreqEncoder();
        if (freqEncoder != null) {
            LOG.debug("{},{} init freq encoding:{}", deltaObjectId, desc.getMeasurementId(),
                    freqEncoder);
            this.dataValueWriter.setFreqEncoder(freqEncoder);
        }
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
        checkPageSize();
    }

    @Override
    public void write(long time, int value) throws IOException {
        this.time = time;
        ++valueCount;
        dataValueWriter.write(time, value);
        pageStatistics.updateStats(value);
        if (minTimestamp == -1)
            minTimestamp = time;
        checkPageSize();
    }

    @Override
    public void write(long time, boolean value) throws IOException {
        this.time = time;
        ++valueCount;
        dataValueWriter.write(time, value);
        pageStatistics.updateStats(value);
        if (minTimestamp == -1)
            minTimestamp = time;
        checkPageSize();
    }

    @Override
    public void write(long time, float value) throws IOException {
        this.time = time;
        ++valueCount;
        dataValueWriter.write(time, value);
        pageStatistics.updateStats(value);
        if (minTimestamp == -1)
            minTimestamp = time;
        checkPageSize();
    }

    @Override
    public void write(long time, double value) throws IOException {
        this.time = time;
        ++valueCount;
        dataValueWriter.write(time, value);
        pageStatistics.updateStats(value);
        if (minTimestamp == -1)
            minTimestamp = time;
        checkPageSize();
    }

    @Override
    public void write(long time, BigDecimal value) throws IOException {
        this.time = time;
        ++valueCount;
        dataValueWriter.write(time, value);
        pageStatistics.updateStats(value);
        if (minTimestamp == -1)
            minTimestamp = time;
        checkPageSize();
    }

    @Override
    public void write(long time, Binary value) throws IOException {
        this.time = time;
        ++valueCount;
        dataValueWriter.write(time, value);
        pageStatistics.updateStats(value);
        if (minTimestamp == -1)
            minTimestamp = time;
        checkPageSize();
    }

    /**
     * check occupied memory size, if it exceeds the PageSize threshold, flush them to given
     * OutputStream.
     *
     */
    private void checkPageSize() {
        if (valueCount > valueCountForNextSizeCheck) {
            // not checking the memory used for every value
            long currentColumnSize = dataValueWriter.estimateMaxMemSize();
            if (currentColumnSize > psThres) {
                // we will write the current page and check again the size at the predicted middle
                // of next page
                valueCountForNextSizeCheck = valueCount / 2;
                LOG.debug("enough size, write page {}", desc);
                writePage();
            } else {
                // not reached the threshold, will check again midway
                valueCountForNextSizeCheck =
                        (int) (valueCount + ((float) valueCount * psThres / currentColumnSize)) / 2 + 1;
                LOG.debug("{}:{} not enough size, now: {}, change to {}", deltaObjectId, desc,
                        valueCount, valueCountForNextSizeCheck);
            }
        }
    }

    /**
     * pack data into {@code IPageWriter}
     */
    private void writePage() {
        try {
            pageWriter.writePage(dataValueWriter.getBytes(), valueCount, pageStatistics, time,
                    minTimestamp);
            this.seriesStatistics.mergeStatistics(this.pageStatistics);
        } catch (IOException e) {
            LOG.error("meet error in dataValueWriter.getBytes(),ignore this page, {}",
                    e.getMessage());
        } catch (PageException e) {
            LOG.error("meet error in pageWriter.writePage,ignore this page, error message:{}",
                    e.getMessage());
        } finally {
            // clear start time stamp for next initializing
            minTimestamp = -1;
            valueCount = 0;
            dataValueWriter.reset();
            resetPageStatistics();
        }
    }

    @Override
    public void writeToFileWriter(TSFileIOWriter tsfileWriter) throws IOException {
        if (valueCount > 0) {
            writePage();
        }
        pageWriter.writeToFileWriter(tsfileWriter, seriesStatistics);
        pageWriter.reset();
        // reset series_statistics
        this.seriesStatistics = Statistics.getStatsByType(dataType);
    }

    @Override
    public long estimateMaxSeriesMemSize() {
        return dataValueWriter.estimateMaxMemSize() + pageWriter.estimateMaxPageMemSize();
    }
}

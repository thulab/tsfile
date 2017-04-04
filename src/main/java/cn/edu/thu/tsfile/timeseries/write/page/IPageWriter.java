package cn.edu.thu.tsfile.timeseries.write.page;

import java.io.IOException;

import cn.edu.thu.tsfile.common.utils.bytesinput.BytesInput;
import cn.edu.thu.tsfile.file.metadata.statistics.Statistics;
import cn.edu.thu.tsfile.timeseries.write.series.ISeriesWriter;
import cn.edu.thu.tsfile.timeseries.write.exception.PageException;
import cn.edu.thu.tsfile.timeseries.write.io.TSFileIOWriter;

/**
 * Each SeriesWriter has a page writer. While memory space occupied by series writer exceeds
 * specified threshold, pack it into a page.
 * 
 * @see ISeriesWriter
 * @author kangrong
 *
 */
public interface IPageWriter {
    /**
     * store a page to this pageWriter with given bytesInput.
     * 
     * @param bytesInput - the data to be stored to pageWriter
     * @param valueCount - the amount of values in that page
     * @param statistics - the statistics for that page
     * @param maxTimestamp - timestamp maximum in given data
     * @param minTimestamp - timestamp minimum in given data
     * @throws IOException
     * @throws PageException 
     */
    void writePage(BytesInput bytesInput, int valueCount, Statistics<?> statistics,
            long maxTimestamp, long minTimestamp) throws PageException;

    /**
     * write the page to specified IOWriter
     * 
     * @param writer - the specified IOWriter
     * @param statistics - the statistic information provided by series writer
     * @throws IOException
     */
    void writeToFileWriter(TSFileIOWriter writer, Statistics<?> statistics) throws IOException;

    /**
     * reset exist data in page for next stage
     */
    void reset();

    /**
     * @return the max possible allocated size
     */
    long estimateMaxPageMemSize();
}

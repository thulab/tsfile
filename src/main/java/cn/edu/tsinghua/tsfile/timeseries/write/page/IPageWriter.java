package cn.edu.tsinghua.tsfile.timeseries.write.page;

import cn.edu.tsinghua.tsfile.file.metadata.statistics.Statistics;
import cn.edu.tsinghua.tsfile.timeseries.write.exception.PageException;
import cn.edu.tsinghua.tsfile.timeseries.write.io.TsFileIOWriter;
import cn.edu.tsinghua.tsfile.timeseries.write.series.ISeriesWriter;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Each SeriesWriter has a page writer. While memory space occupied by series writer exceeds
 * specified threshold, pack it into a page.
 *
 * @author kangrong
 * @see ISeriesWriter
 */
public interface IPageWriter {
    /**
     * store a page to this pageWriter.
     *
     * @param listByteArray - the data to be stored to pageWriter
     * @param valueCount    - the amount of values in that page
     * @param statistics    - the statistics for that page
     * @param maxTimestamp  - timestamp maximum in given data
     * @param minTimestamp  - timestamp minimum in given data
     * @throws PageException - if an PageException occurs.
     */
    void writePageHeaderAndDataIntoBuff(ByteBuffer listByteArray, int valueCount, Statistics<?> statistics,
                                        long maxTimestamp, long minTimestamp) throws PageException;

    /**
     * write the page to specified IOWriter
     *
     * @param writer the specified IOWriter
     * @param statistics the statistic information provided by series writer
     * @throws IOException exception in IO
     */
    void writeAllPagesOfSeriesToTsFile(TsFileIOWriter writer, Statistics<?> statistics) throws IOException;

    /**
     * reset exist data in page for next stage
     */
    void reset();

    /**
     * @return the max possible allocated size
     */
    long estimateMaxPageMemSize();
}

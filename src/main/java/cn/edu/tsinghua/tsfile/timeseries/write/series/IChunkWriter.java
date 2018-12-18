package cn.edu.tsinghua.tsfile.timeseries.write.series;

import java.io.IOException;
import java.math.BigDecimal;

import cn.edu.tsinghua.tsfile.common.utils.Binary;
import cn.edu.tsinghua.tsfile.timeseries.write.io.TsFileIOWriter;

/**
 * IChunkWriter provides a list of writing methods for different value types.
 *
 * @author kangrong
 */
public interface IChunkWriter {

    /**
     * write a time value pair
     * @param time data timestamp
     * @param value MUST be int
     * @throws IOException IOException
     */
    void write(long time, int value) throws IOException;

    /**
     * write a time value pair
     * @param time data timestamp
     * @param value MUST be long
     * @throws IOException IOException
     */
    void write(long time, long value) throws IOException;

    /**
     * write a time value pair
     * @param time data timestamp
     * @param value MUST be boolean
     * @throws IOException IOException
     */
    void write(long time, boolean value) throws IOException;

    /**
     * write a time value pair
     * @param time data timestamp
     * @param value MUST be float
     * @throws IOException IOException
     */
    void write(long time, float value) throws IOException;

    /**
     * write a time value pair
     * @param time data timestamp
     * @param value MUST be double
     * @throws IOException IOException
     */
    void write(long time, double value) throws IOException;

    /**
     * write a time value pair
     * @param time data timestamp
     * @param value MUST be BigDecimal
     * @throws IOException IOException
     */
    void write(long time, BigDecimal value) throws IOException;

    /**
     * write a time value pair
     * @param time data timestamp
     * @param value MUST be Binary
     * @throws IOException IOException
     */
    void write(long time, Binary value) throws IOException;

    /**
     * flush data to TsFileIOWriter
     * @param tsfileWriter Must be TsFileIOWriter
     * @throws IOException IOException
     */
    void writeToFileWriter(TsFileIOWriter tsfileWriter) throws IOException;

    /**
     * estimate memory used size of this series
     * @return estimated max series memory size
     */
    long estimateMaxSeriesMemSize();

    /**
     * return the serialized size of the chunk header + all pages
     * @return current chunk size
     */
    long getCurrentChunkSize();

    /**
     * prepare to flush data into file.
     */
    void preFlush();

    int getNumOfPages();
}

package cn.edu.tsinghua.tsfile.timeseries.read.reader;

import cn.edu.tsinghua.tsfile.timeseries.read.datatype.TimeValuePair;

import java.io.IOException;

/**
 * @author Jinrui Zhang
 */
public interface SeriesReader {

    /**
     *
     * @return if there is a next time-value pair
     * @throws IOException IOException
     */
    boolean hasNext() throws IOException;

    /**
     *
     * @return next time value pair
     * @throws IOException IOException
     */
    TimeValuePair next() throws IOException;

    /**
     * skip the current time value pair, just call next()
     * @throws IOException IOException
     */
    void skipCurrentTimeValuePair() throws IOException;

    void close() throws IOException;
}


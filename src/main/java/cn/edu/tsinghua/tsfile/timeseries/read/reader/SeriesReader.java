package cn.edu.tsinghua.tsfile.timeseries.read.reader;

import cn.edu.tsinghua.tsfile.timeseries.read.datatype.TimeValuePair;

import java.io.IOException;

/**
 * @author Jinrui Zhang
 */
public interface SeriesReader {

    /**
     * if there is a next time-value pair
     */
    boolean hasNext() throws IOException;

    /**
     * @return next time value pair
     */
    TimeValuePair next() throws IOException;

    /**
     * skip the current time value pair, just call next()
     */
    void skipCurrentTimeValuePair() throws IOException;

    void close() throws IOException;
}


package cn.edu.tsinghua.tsfile.timeseries.read.reader;

import cn.edu.tsinghua.tsfile.timeseries.read.datatype.TsPrimitiveType;

import java.io.IOException;

public interface SeriesReaderByTimeStamp extends SeriesReader {
    /**
     * @param timestamp
     * @return If there is no TimeValuePair whose timestamp equals to given timestamp, then return null.
     * @throws IOException
     */
    TsPrimitiveType getValueInTimestamp(long timestamp) throws IOException;

}

package cn.edu.tsinghua.tsfile.timeseries.readV2.reader;

public interface SeriesReaderByTimeStamp extends SeriesReader{
    /**
     * Set time value of the time-value pair to fetch
     * */
    void setCurrentTimestamp(long timestamp);

}

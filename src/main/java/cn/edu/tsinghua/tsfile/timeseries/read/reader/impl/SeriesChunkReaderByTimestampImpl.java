package cn.edu.tsinghua.tsfile.timeseries.read.reader.impl;

import cn.edu.tsinghua.tsfile.file.header.PageHeader;
import cn.edu.tsinghua.tsfile.timeseries.read.datatype.TimeValuePair;
import cn.edu.tsinghua.tsfile.timeseries.read.datatype.TsPrimitiveType;
import cn.edu.tsinghua.tsfile.timeseries.read.reader.SeriesReaderByTimeStamp;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Created by zhangjinrui on 2017/12/26.
 */
public class SeriesChunkReaderByTimestampImpl extends SeriesChunkReader implements SeriesReaderByTimeStamp{

    private long currentTimestamp;

    public SeriesChunkReaderByTimestampImpl(ByteBuffer seriesChunkInputStream) {
        super(seriesChunkInputStream);
    }

    @Override
    public boolean pageSatisfied(PageHeader pageHeader) {
        long maxTimestamp = pageHeader.getMax_timestamp();
        //If minTimestamp > currentTimestamp, this page should NOT be skipped
        return maxTimestamp >= currentTimestamp && maxTimestamp >= getMaxTombstoneTime();
    }

    @Override
    public boolean timeValuePairSatisfied(TimeValuePair timeValuePair) {
        return timeValuePair.getTimestamp() >= currentTimestamp && timeValuePair.getTimestamp() > getMaxTombstoneTime();
    }

    public void setCurrentTimestamp(long currentTimestamp) {
        this.currentTimestamp = currentTimestamp;
        if(hasCachedTimeValuePair && cachedTimeValuePair.getTimestamp() < currentTimestamp){
            hasCachedTimeValuePair = false;
        }
    }

    @Override
    public TsPrimitiveType getValueInTimestamp(long timestamp) throws IOException {
        setCurrentTimestamp(timestamp);
        if(hasCachedTimeValuePair && cachedTimeValuePair.getTimestamp() == timestamp){
            hasCachedTimeValuePair = false;
            return cachedTimeValuePair.getValue();
        }
        while (hasNext()){
            cachedTimeValuePair = next();
            if(cachedTimeValuePair.getTimestamp() == timestamp){
                return cachedTimeValuePair.getValue();
            }
            else if(cachedTimeValuePair.getTimestamp() > timestamp){
                hasCachedTimeValuePair = true;
                return null;
            }
        }
        return null;
    }
}

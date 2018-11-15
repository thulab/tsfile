package cn.edu.tsinghua.tsfile.timeseries.read.reader.impl;

import cn.edu.tsinghua.tsfile.file.header.PageHeader;
import cn.edu.tsinghua.tsfile.timeseries.read.datatype.TimeValuePair;

import java.nio.ByteBuffer;

/**
 * Created by zhangjinrui on 2017/12/24.
 */
public class SeriesChunkReaderWithoutFilterImpl extends SeriesChunkReader {

    public SeriesChunkReaderWithoutFilterImpl(ByteBuffer seriesChunkInputStream) {
        super(seriesChunkInputStream);
    }

    @Override
    public boolean pageSatisfied(PageHeader pageHeader) {
        return pageHeader.getMax_timestamp() > getMaxTombstoneTime();
    }

    @Override
    public boolean timeValuePairSatisfied(TimeValuePair timeValuePair) {
        return timeValuePair.getTimestamp() > getMaxTombstoneTime();
    }
}

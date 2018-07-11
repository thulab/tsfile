package cn.edu.tsinghua.tsfile.timeseries.readV2.reader.impl;

import cn.edu.tsinghua.tsfile.file.header.PageHeader;
import cn.edu.tsinghua.tsfile.file.metadata.enums.CompressionType;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSEncoding;
import cn.edu.tsinghua.tsfile.timeseries.readV2.datatype.TimeValuePair;

import java.io.InputStream;

/**
 * Created by zhangjinrui on 2017/12/26.
 */
public class SeriesChunkReaderByTimestampImpl extends SeriesChunkReader {

    private long currentTimestamp;

    public SeriesChunkReaderByTimestampImpl(InputStream seriesChunkInputStream) {
        super(seriesChunkInputStream);
    }

    @Override
    public boolean pageSatisfied(PageHeader pageHeader) {
        long maxTimestamp = pageHeader.getMax_timestamp();
        //If minTimestamp > currentTimestamp, this page should NOT be skipped
        if (maxTimestamp < currentTimestamp) {
            return false;
        }
        return true;
    }

    @Override
    public boolean timeValuePairSatisfied(TimeValuePair timeValuePair) {
        return true;
    }

    public void setCurrentTimestamp(long currentTimestamp) {
        this.currentTimestamp = currentTimestamp;
    }
}

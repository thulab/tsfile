package cn.edu.tsinghua.tsfile.timeseries.readV2.reader.impl;

import cn.edu.tsinghua.tsfile.file.metadata.enums.CompressionTypeName;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.format.PageHeader;
import cn.edu.tsinghua.tsfile.timeseries.readV2.datatype.TimeValuePair;

import java.io.InputStream;

/**
 * Created by zhangjinrui on 2017/12/24.
 */
public class SeriesChunkReaderWithoutFilterImpl extends SeriesChunkReader {

    public SeriesChunkReaderWithoutFilterImpl(InputStream seriesChunkInputStream, TSDataType dataType, CompressionTypeName compressionTypeName) {
        super(seriesChunkInputStream, dataType, compressionTypeName);
    }

    @Override
    public boolean pageSatisfied(PageHeader pageHeader) {
        return true;
    }

    @Override
    public boolean timeValuePairSatisfied(TimeValuePair timeValuePair) {
        return true;
    }
}

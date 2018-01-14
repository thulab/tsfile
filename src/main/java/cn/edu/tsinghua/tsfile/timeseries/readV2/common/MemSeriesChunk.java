package cn.edu.tsinghua.tsfile.timeseries.readV2.common;

import java.io.ByteArrayInputStream;

/**
 * Created by zhangjinrui on 2017/12/25.
 */
public class MemSeriesChunk {
    private SeriesChunkDescriptor seriesChunkDescriptor;
    private ByteArrayInputStream seriesChunkBodyStream;

    public MemSeriesChunk(SeriesChunkDescriptor seriesChunkDescriptor, ByteArrayInputStream seriesChunkBodyStream) {
        this.seriesChunkDescriptor = seriesChunkDescriptor;
        this.seriesChunkBodyStream = seriesChunkBodyStream;
    }

    public SeriesChunkDescriptor getSeriesChunkDescriptor() {
        return seriesChunkDescriptor;
    }

    public ByteArrayInputStream getSeriesChunkBodyStream() {
        return seriesChunkBodyStream;
    }
}

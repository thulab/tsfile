package cn.edu.tsinghua.tsfile.timeseries.read.common;

import cn.edu.tsinghua.tsfile.file.metadata.TimeSeriesChunkMetaData;

import java.nio.ByteBuffer;

/**
 * Created by zhangjinrui on 2017/12/25.
 */
public class MemSeriesChunk implements SeriesChunk{
    private TimeSeriesChunkMetaData timeSeriesChunkMetaData;
    private ByteBuffer seriesChunkBodyStream;

    public MemSeriesChunk(TimeSeriesChunkMetaData timeSeriesChunkMetaData, ByteBuffer seriesChunkBodyStream) {
        this.timeSeriesChunkMetaData = timeSeriesChunkMetaData;
        this.seriesChunkBodyStream = seriesChunkBodyStream;
    }

    public TimeSeriesChunkMetaData getTimeSeriesChunkMetaData() {
        return timeSeriesChunkMetaData;
    }

    public ByteBuffer getSeriesChunkBodyStream() {
        return seriesChunkBodyStream;
    }
}

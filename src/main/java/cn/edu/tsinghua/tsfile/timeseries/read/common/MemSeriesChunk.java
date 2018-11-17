package cn.edu.tsinghua.tsfile.timeseries.read.common;

import cn.edu.tsinghua.tsfile.file.metadata.ChunkMetaData;

import java.nio.ByteBuffer;

/**
 * Created by zhangjinrui on 2017/12/25.
 */
public class MemSeriesChunk implements SeriesChunk{
    private ChunkMetaData chunkMetaData;
    private ByteBuffer seriesChunkBodyStream;

    public MemSeriesChunk(ChunkMetaData chunkMetaData, ByteBuffer seriesChunkBodyStream) {
        this.chunkMetaData = chunkMetaData;
        this.seriesChunkBodyStream = seriesChunkBodyStream;
    }

    public ChunkMetaData getChunkMetaData() {
        return chunkMetaData;
    }

    public ByteBuffer getSeriesChunkBodyStream() {
        return seriesChunkBodyStream;
    }
}

package cn.edu.tsinghua.tsfile.timeseries.read.common;

import cn.edu.tsinghua.tsfile.file.metadata.ChunkMetaData;

import java.nio.ByteBuffer;

/**
 *
 * Created by zhangjinrui on 2018/1/14.
 */
public interface SeriesChunk {

    ChunkMetaData getChunkMetaData();

    ByteBuffer getSeriesChunkBodyStream();
}

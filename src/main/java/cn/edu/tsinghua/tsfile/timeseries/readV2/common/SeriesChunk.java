package cn.edu.tsinghua.tsfile.timeseries.readV2.common;

import cn.edu.tsinghua.tsfile.file.metadata.TimeSeriesChunkMetaData;

import java.nio.ByteBuffer;

/**
 *
 * Created by zhangjinrui on 2018/1/14.
 */
public interface SeriesChunk {

    TimeSeriesChunkMetaData getTimeSeriesChunkMetaData();

    ByteBuffer getSeriesChunkBodyStream();
}

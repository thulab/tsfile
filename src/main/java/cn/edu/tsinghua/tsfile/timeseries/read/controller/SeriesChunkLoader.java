package cn.edu.tsinghua.tsfile.timeseries.read.controller;

import cn.edu.tsinghua.tsfile.timeseries.read.common.SeriesChunk;
import cn.edu.tsinghua.tsfile.file.metadata.TimeSeriesChunkMetaData;

import java.io.IOException;

/**
 * Created by zhangjinrui on 2017/12/26.
 */
public interface SeriesChunkLoader {

    /**
     * read all content of any chunk
     */
    SeriesChunk getMemSeriesChunk(TimeSeriesChunkMetaData timeSeriesChunkMetaData) throws IOException;
}

package cn.edu.tsinghua.tsfile.timeseries.read.controller;

import cn.edu.tsinghua.tsfile.timeseries.read.common.SeriesChunk;
import cn.edu.tsinghua.tsfile.file.metadata.TimeSeriesChunkMetaData;

import java.io.IOException;

/**
 * 可以读一个流中的任意chunk（可以是不同的time series的）
 * TODO 这是一个应该被作废的类
 * Created by zhangjinrui on 2017/12/26.
 */
public interface SeriesChunkLoader {
    /**
     * 将该chunk的数据从磁盘上全部读出并放在内存中返回。
     * @param timeSeriesChunkMetaData
     * @return
     * @throws IOException
     */
    SeriesChunk getMemSeriesChunk(TimeSeriesChunkMetaData timeSeriesChunkMetaData) throws IOException;
}

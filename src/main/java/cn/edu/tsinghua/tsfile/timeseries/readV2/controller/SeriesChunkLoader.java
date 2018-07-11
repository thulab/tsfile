package cn.edu.tsinghua.tsfile.timeseries.readV2.controller;

import cn.edu.tsinghua.tsfile.timeseries.readV2.common.SeriesChunk;
import cn.edu.tsinghua.tsfile.timeseries.readV2.common.EncodedSeriesChunkDescriptor;

import java.io.IOException;

/**
 * 可以读一个流中的任意chunk（可以是不同的time series的）
 * TODO 这是一个应该被作废的类
 * Created by zhangjinrui on 2017/12/26.
 */
public interface SeriesChunkLoader {
    SeriesChunk getMemSeriesChunk(EncodedSeriesChunkDescriptor encodedSeriesChunkDescriptor) throws IOException;
}

package cn.edu.tsinghua.tsfile.timeseries.readV2.controller;

import cn.edu.tsinghua.tsfile.timeseries.readV2.common.MemSeriesChunk;
import cn.edu.tsinghua.tsfile.timeseries.readV2.common.SeriesChunkDescriptor;

import java.io.IOException;

/**
 * Created by zhangjinrui on 2017/12/26.
 */
public interface SeriesChunkLoader {
    MemSeriesChunk getMemSeriesChunk(SeriesChunkDescriptor seriesChunkDescriptor) throws IOException;
}

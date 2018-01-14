package cn.edu.tsinghua.tsfile.timeseries.readV2.controller;

import cn.edu.tsinghua.tsfile.timeseries.read.support.Path;
import cn.edu.tsinghua.tsfile.timeseries.readV2.common.SeriesChunkDescriptor;

import java.io.IOException;
import java.util.List;

/**
 * Created by zhangjinrui on 2017/12/25.
 */
public interface MetadataQuerier {

    List<SeriesChunkDescriptor> getSeriesChunkDescriptorList(Path path) throws IOException;

}

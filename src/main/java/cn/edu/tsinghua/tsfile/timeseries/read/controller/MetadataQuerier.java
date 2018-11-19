package cn.edu.tsinghua.tsfile.timeseries.read.controller;

import cn.edu.tsinghua.tsfile.file.metadata.ChunkMetaData;
import cn.edu.tsinghua.tsfile.file.metadata.TsFileMetaData;
import cn.edu.tsinghua.tsfile.timeseries.read.common.Path;

import java.io.IOException;
import java.util.List;

/**
 * Created by zhangjinrui on 2017/12/25.
 */
public interface MetadataQuerier {

    List<ChunkMetaData> getSeriesChunkMetaDataList(Path path) throws IOException;
    public TsFileMetaData getWholeFileMetadata();

}

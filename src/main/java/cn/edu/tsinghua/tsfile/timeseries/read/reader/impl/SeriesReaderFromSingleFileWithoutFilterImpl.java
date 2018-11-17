package cn.edu.tsinghua.tsfile.timeseries.read.reader.impl;

import cn.edu.tsinghua.tsfile.file.metadata.ChunkMetaData;
import cn.edu.tsinghua.tsfile.timeseries.read.TsFileSequenceReader;
import cn.edu.tsinghua.tsfile.timeseries.read.common.Path;
import cn.edu.tsinghua.tsfile.timeseries.read.common.SeriesChunk;
import cn.edu.tsinghua.tsfile.timeseries.read.controller.SeriesChunkLoader;

import java.io.IOException;
import java.util.List;

/**
 * Created by zhangjinrui on 2017/12/25.
 */
public class SeriesReaderFromSingleFileWithoutFilterImpl extends SeriesReaderFromSingleFile {

    public SeriesReaderFromSingleFileWithoutFilterImpl(SeriesChunkLoader seriesChunkLoader, List<ChunkMetaData> chunkMetaDataList) {
        super(seriesChunkLoader, chunkMetaDataList);
    }

    public SeriesReaderFromSingleFileWithoutFilterImpl(TsFileSequenceReader tsFileReader, Path path) throws IOException {
        super(tsFileReader, path);
    }

    public SeriesReaderFromSingleFileWithoutFilterImpl(TsFileSequenceReader tsFileReader,
                                      SeriesChunkLoader seriesChunkLoader, List<ChunkMetaData> chunkMetaDataList) {
        super(tsFileReader, seriesChunkLoader, chunkMetaDataList);
    }

    protected void initSeriesChunkReader(ChunkMetaData chunkMetaData) throws IOException {
        SeriesChunk memSeriesChunk = seriesChunkLoader.getMemSeriesChunk(chunkMetaData);
        this.seriesChunkReader = new SeriesChunkReaderWithoutFilterImpl(memSeriesChunk.getSeriesChunkBodyStream());
 		this.seriesChunkReader.setMaxTombstoneTime(chunkMetaData.getMaxTombstoneTime());
    }

    @Override
    protected boolean seriesChunkSatisfied(ChunkMetaData chunkMetaData) {
        return true;
    }
}

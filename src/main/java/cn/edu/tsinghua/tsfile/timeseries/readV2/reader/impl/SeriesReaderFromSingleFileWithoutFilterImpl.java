package cn.edu.tsinghua.tsfile.timeseries.readV2.reader.impl;

import cn.edu.tsinghua.tsfile.timeseries.readV2.TsFileSequenceReader;
import cn.edu.tsinghua.tsfile.file.metadata.TimeSeriesChunkMetaData;
import cn.edu.tsinghua.tsfile.timeseries.readV2.common.Path;
import cn.edu.tsinghua.tsfile.timeseries.readV2.common.SeriesChunk;
import cn.edu.tsinghua.tsfile.timeseries.readV2.controller.SeriesChunkLoader;

import java.io.IOException;
import java.util.List;

/**
 * Created by zhangjinrui on 2017/12/25.
 */
public class SeriesReaderFromSingleFileWithoutFilterImpl extends SeriesReaderFromSingleFile {

    public SeriesReaderFromSingleFileWithoutFilterImpl(SeriesChunkLoader seriesChunkLoader, List<TimeSeriesChunkMetaData> timeSeriesChunkMetaDataList) {
        super(seriesChunkLoader, timeSeriesChunkMetaDataList);
    }

    public SeriesReaderFromSingleFileWithoutFilterImpl(TsFileSequenceReader tsFileReader, Path path) throws IOException {
        super(tsFileReader, path);
    }

    public SeriesReaderFromSingleFileWithoutFilterImpl(TsFileSequenceReader tsFileReader,
                                      SeriesChunkLoader seriesChunkLoader, List<TimeSeriesChunkMetaData> timeSeriesChunkMetaDataList) {
        super(tsFileReader, seriesChunkLoader, timeSeriesChunkMetaDataList);
    }

    protected void initSeriesChunkReader(TimeSeriesChunkMetaData timeSeriesChunkMetaData) throws IOException {
        SeriesChunk memSeriesChunk = seriesChunkLoader.getMemSeriesChunk(timeSeriesChunkMetaData);
        this.seriesChunkReader = new SeriesChunkReaderWithoutFilterImpl(memSeriesChunk.getSeriesChunkBodyStream());
 		this.seriesChunkReader.setMaxTombstoneTime(timeSeriesChunkMetaData.getMaxTombstoneTime());
    }

    @Override
    protected boolean seriesChunkSatisfied(TimeSeriesChunkMetaData timeSeriesChunkMetaData) {
        return true;
    }
}

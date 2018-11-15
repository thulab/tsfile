package cn.edu.tsinghua.tsfile.timeseries.read.reader.impl;

import cn.edu.tsinghua.tsfile.timeseries.read.TsFileSequenceReader;
import cn.edu.tsinghua.tsfile.file.metadata.TimeSeriesChunkMetaData;
import cn.edu.tsinghua.tsfile.timeseries.read.common.Path;
import cn.edu.tsinghua.tsfile.timeseries.read.controller.MetadataQuerierByFileImpl;
import cn.edu.tsinghua.tsfile.timeseries.read.controller.SeriesChunkLoader;
import cn.edu.tsinghua.tsfile.timeseries.read.controller.SeriesChunkLoaderImpl;
import cn.edu.tsinghua.tsfile.timeseries.read.datatype.TimeValuePair;
import cn.edu.tsinghua.tsfile.timeseries.read.reader.SeriesReader;

import java.io.IOException;
import java.util.List;

/**
 * Created by zhangjinrui on 2017/12/25.
 */
public abstract class SeriesReaderFromSingleFile implements SeriesReader {

    protected SeriesChunkLoader seriesChunkLoader;
    protected List<TimeSeriesChunkMetaData> timeSeriesChunkMetaDataList;

    protected SeriesChunkReader seriesChunkReader;
    protected boolean seriesChunkReaderInitialized;
    protected int currentReadSeriesChunkIndex;

    protected TsFileSequenceReader fileReader;

    public SeriesReaderFromSingleFile(TsFileSequenceReader fileReader, Path path) throws IOException {
        this.fileReader = fileReader;
        this.seriesChunkLoader = new SeriesChunkLoaderImpl(fileReader);
        this.timeSeriesChunkMetaDataList = new MetadataQuerierByFileImpl(fileReader).getSeriesChunkMetaDataList(path);
        this.currentReadSeriesChunkIndex = -1;
        this.seriesChunkReaderInitialized = false;
    }

    public SeriesReaderFromSingleFile(TsFileSequenceReader fileReader,
                                      SeriesChunkLoader seriesChunkLoader, List<TimeSeriesChunkMetaData> timeSeriesChunkMetaDataList) {
        this(seriesChunkLoader, timeSeriesChunkMetaDataList);
        this.fileReader = fileReader;
    }

    /**
     * Using this constructor cannot close corresponding FileStream
     * @param seriesChunkLoader
     * @param timeSeriesChunkMetaDataList
     */
    public SeriesReaderFromSingleFile(SeriesChunkLoader seriesChunkLoader, List<TimeSeriesChunkMetaData> timeSeriesChunkMetaDataList) {
        this.seriesChunkLoader = seriesChunkLoader;
        this.timeSeriesChunkMetaDataList = timeSeriesChunkMetaDataList;
        this.currentReadSeriesChunkIndex = -1;
        this.seriesChunkReaderInitialized = false;
    }

    @Override
    public boolean hasNext() throws IOException {
        if (seriesChunkReaderInitialized && seriesChunkReader.hasNext()) {
            return true;
        }
        while ((currentReadSeriesChunkIndex + 1) < timeSeriesChunkMetaDataList.size()) {
            if (!seriesChunkReaderInitialized) {
                TimeSeriesChunkMetaData timeSeriesChunkMetaData = timeSeriesChunkMetaDataList.get(++currentReadSeriesChunkIndex);
                if (seriesChunkSatisfied(timeSeriesChunkMetaData)) {
                    initSeriesChunkReader(timeSeriesChunkMetaData);
                    seriesChunkReaderInitialized = true;
                } else {
                    continue;
                }
            }
            if (seriesChunkReader.hasNext()) {
                return true;
            } else {
                seriesChunkReaderInitialized = false;
            }
        }
        return false;
    }

    @Override
    public TimeValuePair next() throws IOException {
        return seriesChunkReader.next();
    }

    @Override
    public void skipCurrentTimeValuePair() throws IOException {
        next();
    }

    protected abstract void initSeriesChunkReader(TimeSeriesChunkMetaData timeSeriesChunkMetaData) throws IOException;

    protected abstract boolean seriesChunkSatisfied(TimeSeriesChunkMetaData timeSeriesChunkMetaData);

    public void close() throws IOException {
        if (fileReader != null) {
            fileReader.close();
        }
    }
}

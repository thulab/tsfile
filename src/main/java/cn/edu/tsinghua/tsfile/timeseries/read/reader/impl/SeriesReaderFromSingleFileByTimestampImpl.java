package cn.edu.tsinghua.tsfile.timeseries.read.reader.impl;

import cn.edu.tsinghua.tsfile.timeseries.read.TsFileSequenceReader;
import cn.edu.tsinghua.tsfile.file.metadata.TimeSeriesChunkMetaData;
import cn.edu.tsinghua.tsfile.timeseries.read.common.Path;
import cn.edu.tsinghua.tsfile.timeseries.read.common.SeriesChunk;
import cn.edu.tsinghua.tsfile.timeseries.read.controller.SeriesChunkLoader;
import cn.edu.tsinghua.tsfile.timeseries.read.datatype.TimeValuePair;
import cn.edu.tsinghua.tsfile.timeseries.read.datatype.TsPrimitiveType;
import cn.edu.tsinghua.tsfile.timeseries.read.reader.SeriesReaderByTimeStamp;

import java.io.IOException;
import java.util.List;

/**
 * Created by zhangjinrui on 2017/12/26.
 */
public class SeriesReaderFromSingleFileByTimestampImpl extends SeriesReaderFromSingleFile implements SeriesReaderByTimeStamp {

    private long currentTimestamp;
    private boolean hasCacheLastTimeValuePair;
    private TimeValuePair cachedTimeValuePair;
    private int nextSeriesChunkIndex;

    public SeriesReaderFromSingleFileByTimestampImpl(SeriesChunkLoader seriesChunkLoader, List<TimeSeriesChunkMetaData> timeSeriesChunkMetaDataList) {
        super(seriesChunkLoader, timeSeriesChunkMetaDataList);
        nextSeriesChunkIndex = 0;
        currentTimestamp = Long.MIN_VALUE;
    }

    public SeriesReaderFromSingleFileByTimestampImpl(TsFileSequenceReader tsFileReader, Path path) throws IOException {
        super(tsFileReader, path);
        currentTimestamp = Long.MIN_VALUE;
    }

    public SeriesReaderFromSingleFileByTimestampImpl(TsFileSequenceReader tsFileReader,
                                      SeriesChunkLoader seriesChunkLoader, List<TimeSeriesChunkMetaData> timeSeriesChunkMetaDataList) {
        super(tsFileReader, seriesChunkLoader, timeSeriesChunkMetaDataList);
        currentTimestamp = Long.MIN_VALUE;
    }

    @Override
    public boolean hasNext() throws IOException {
        if (hasCacheLastTimeValuePair && cachedTimeValuePair.getTimestamp() >= currentTimestamp) {
            return true;
        }
        if (seriesChunkReaderInitialized) {
            ((SeriesChunkReaderByTimestampImpl) seriesChunkReader).setCurrentTimestamp(currentTimestamp);
            if(seriesChunkReader.hasNext()){
                return true;
            }
        }
        while (nextSeriesChunkIndex < timeSeriesChunkMetaDataList.size()) {
            if (!seriesChunkReaderInitialized) {
                TimeSeriesChunkMetaData timeSeriesChunkMetaData = timeSeriesChunkMetaDataList.get(nextSeriesChunkIndex);
                //maxTime >= currentTime
                if (seriesChunkSatisfied(timeSeriesChunkMetaData)) {
                    initSeriesChunkReader(timeSeriesChunkMetaData);
                    ((SeriesChunkReaderByTimestampImpl) seriesChunkReader).setCurrentTimestamp(currentTimestamp);
                    seriesChunkReaderInitialized = true;
                    nextSeriesChunkIndex++;
                } else {
                    long minTimestamp = timeSeriesChunkMetaData.getStartTime();
                    long maxTimestamp = timeSeriesChunkMetaData.getEndTime();
                    if (maxTimestamp < currentTimestamp) {
                        continue;
                    } else if (minTimestamp > currentTimestamp) {
                        return false;
                    }
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
        if (hasCacheLastTimeValuePair) {
            hasCacheLastTimeValuePair = false;
            return cachedTimeValuePair;
        }
        return seriesChunkReader.next();
    }

    /**
     * @param timestamp
     * @return If there is no TimeValuePair whose timestamp equals to given timestamp, then return null.
     * @throws IOException
     */
    @Override
    public TsPrimitiveType getValueInTimestamp(long timestamp) throws IOException {
        this.currentTimestamp = timestamp;
        if (hasCacheLastTimeValuePair) {
            if (cachedTimeValuePair.getTimestamp() == timestamp) {
                hasCacheLastTimeValuePair = false;
                return cachedTimeValuePair.getValue();
            } else if (cachedTimeValuePair.getTimestamp() > timestamp) {
                return null;
            }
        }
        if(hasNext()){
            cachedTimeValuePair = next();
            if (cachedTimeValuePair.getTimestamp() == timestamp) {
                return cachedTimeValuePair.getValue();
            } else if (cachedTimeValuePair.getTimestamp() > timestamp) {
                hasCacheLastTimeValuePair = true;
                return null;
            }
        }
        return null;
    }

    @Override
    protected void initSeriesChunkReader(TimeSeriesChunkMetaData timeSeriesChunkMetaData) throws IOException {
        SeriesChunk memSeriesChunk = seriesChunkLoader.getMemSeriesChunk(timeSeriesChunkMetaData);
        this.seriesChunkReader = new SeriesChunkReaderByTimestampImpl(memSeriesChunk.getSeriesChunkBodyStream());
        this.seriesChunkReader.setMaxTombstoneTime(timeSeriesChunkMetaData.getMaxTombstoneTime());
    }

    @Override
    protected boolean seriesChunkSatisfied(TimeSeriesChunkMetaData timeSeriesChunkMetaData) {
        long maxTimestamp = timeSeriesChunkMetaData.getEndTime();
        return  maxTimestamp >= currentTimestamp;
    }
}

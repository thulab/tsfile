package cn.edu.tsinghua.tsfile.timeseries.readV2.reader.impl;

import cn.edu.tsinghua.tsfile.timeseries.read.support.Path;
import cn.edu.tsinghua.tsfile.timeseries.readV2.TsFileSequenceReader;
import cn.edu.tsinghua.tsfile.timeseries.readV2.common.EncodedSeriesChunkDescriptor;
import cn.edu.tsinghua.tsfile.timeseries.readV2.common.SeriesChunk;
import cn.edu.tsinghua.tsfile.timeseries.readV2.controller.SeriesChunkLoader;
import cn.edu.tsinghua.tsfile.timeseries.readV2.datatype.TimeValuePair;
import cn.edu.tsinghua.tsfile.timeseries.readV2.datatype.TsPrimitiveType;

import java.io.IOException;
import java.util.List;

/**
 * Created by zhangjinrui on 2017/12/26.
 */
public class SeriesReaderFromSingleFileByTimestampImpl extends SeriesReaderFromSingleFile {

    private long currentTimestamp;
    private boolean hasCacheLastTimeValuePair;
    private TimeValuePair cachedTimeValuePair;
    private int nextSeriesChunkIndex;

    public SeriesReaderFromSingleFileByTimestampImpl(SeriesChunkLoader seriesChunkLoader, List<EncodedSeriesChunkDescriptor> encodedSeriesChunkDescriptorList) {
        super(seriesChunkLoader, encodedSeriesChunkDescriptorList);
        nextSeriesChunkIndex = 0;
    }

    public SeriesReaderFromSingleFileByTimestampImpl(TsFileSequenceReader tsFileReader, Path path) throws IOException {
        super(tsFileReader, path);
    }

    public SeriesReaderFromSingleFileByTimestampImpl(TsFileSequenceReader tsFileReader,
                                      SeriesChunkLoader seriesChunkLoader, List<EncodedSeriesChunkDescriptor> encodedSeriesChunkDescriptorList) {
        super(tsFileReader, seriesChunkLoader, encodedSeriesChunkDescriptorList);
    }

    @Override
    public boolean hasNext() throws IOException {
        if (seriesChunkReaderInitialized && seriesChunkReader.hasNext()) {
            return true;
        }
        while (nextSeriesChunkIndex < encodedSeriesChunkDescriptorList.size()) {
            if (!seriesChunkReaderInitialized) {
                EncodedSeriesChunkDescriptor encodedSeriesChunkDescriptor = encodedSeriesChunkDescriptorList.get(nextSeriesChunkIndex);
                if (seriesChunkSatisfied(encodedSeriesChunkDescriptor)) {
                    initSeriesChunkReader(encodedSeriesChunkDescriptor);
                    ((SeriesChunkReaderByTimestampImpl) seriesChunkReader).setCurrentTimestamp(currentTimestamp);
                    seriesChunkReaderInitialized = true;
                    nextSeriesChunkIndex++;
                } else {
                    long minTimestamp = encodedSeriesChunkDescriptor.getMinTimestamp();
                    long maxTimestamp = encodedSeriesChunkDescriptor.getMaxTimestamp();
                    if (maxTimestamp < currentTimestamp) {
                        continue;
                    } else if (minTimestamp > currentTimestamp) {//TODO 为什么?
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

    /**
     * @param timestamp
     * @return If there is no TimeValuePair whose timestamp equals to given timestamp, then return null.
     * @throws IOException
     */
    public TsPrimitiveType getValueInTimestamp(long timestamp) throws IOException {
        if (hasCacheLastTimeValuePair) {
            if (cachedTimeValuePair.getTimestamp() == timestamp) {
                hasCacheLastTimeValuePair = false;
                return cachedTimeValuePair.getValue();
            } else if (cachedTimeValuePair.getTimestamp() > timestamp) {
                return null;
            }
        }
        if (seriesChunkReaderInitialized) {
            ((SeriesChunkReaderByTimestampImpl) seriesChunkReader).setCurrentTimestamp(timestamp);
        }
        this.currentTimestamp = timestamp;
        while (hasNext()) {
            TimeValuePair timeValuePair = next();
            if (timeValuePair.getTimestamp() == timestamp) {
                return timeValuePair.getValue();
            } else if (timeValuePair.getTimestamp() > timestamp) {
                hasCacheLastTimeValuePair = true;
                cachedTimeValuePair = timeValuePair;
                return null;
            }
        }
        return null;
    }

    @Override
    protected void initSeriesChunkReader(EncodedSeriesChunkDescriptor encodedSeriesChunkDescriptor) throws IOException {
        SeriesChunk memSeriesChunk = seriesChunkLoader.getMemSeriesChunk(encodedSeriesChunkDescriptor);
        this.seriesChunkReader = new SeriesChunkReaderByTimestampImpl(memSeriesChunk.getSeriesChunkBodyStream());
        this.seriesChunkReader.setMaxTombstoneTime(encodedSeriesChunkDescriptor.getMaxTombstoneTime());
    }

    @Override
    protected boolean seriesChunkSatisfied(EncodedSeriesChunkDescriptor encodedSeriesChunkDescriptor) {
        long minTimestamp = encodedSeriesChunkDescriptor.getMinTimestamp();
        long maxTimestamp = encodedSeriesChunkDescriptor.getMaxTimestamp();
        if (minTimestamp <= currentTimestamp && currentTimestamp <= maxTimestamp) {
            return true;
        }
        return false;
    }
}

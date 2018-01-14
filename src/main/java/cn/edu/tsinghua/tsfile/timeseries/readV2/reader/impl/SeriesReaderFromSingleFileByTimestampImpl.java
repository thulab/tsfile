package cn.edu.tsinghua.tsfile.timeseries.readV2.reader.impl;

import cn.edu.tsinghua.tsfile.timeseries.readV2.common.MemSeriesChunk;
import cn.edu.tsinghua.tsfile.timeseries.readV2.common.SeriesChunkDescriptor;
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

    public SeriesReaderFromSingleFileByTimestampImpl(SeriesChunkLoader seriesChunkLoader, List<SeriesChunkDescriptor> seriesChunkDescriptorList) {
        super(seriesChunkLoader, seriesChunkDescriptorList);
        nextSeriesChunkIndex = 0;
    }

    @Override
    public boolean hasNext() throws IOException {
        if (seriesChunkReaderInitialized && seriesChunkReader.hasNext()) {
            return true;
        }
        while (nextSeriesChunkIndex < seriesChunkDescriptorList.size()) {
            if (!seriesChunkReaderInitialized) {
                SeriesChunkDescriptor seriesChunkDescriptor = seriesChunkDescriptorList.get(nextSeriesChunkIndex);
                if (seriesChunkSatisfied(seriesChunkDescriptor)) {
                    initSeriesChunkReader(seriesChunkDescriptor);
                    ((SeriesChunkReaderByTimestampImpl) seriesChunkReader).setCurrentTimestamp(currentTimestamp);
                    seriesChunkReaderInitialized = true;
                    nextSeriesChunkIndex++;
                } else {
                    long minTimestamp = seriesChunkDescriptor.getMinTimestamp();
                    long maxTimestamp = seriesChunkDescriptor.getMaxTimestamp();
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
    protected void initSeriesChunkReader(SeriesChunkDescriptor seriesChunkDescriptor) throws IOException {
        MemSeriesChunk memSeriesChunk = seriesChunkLoader.getMemSeriesChunk(seriesChunkDescriptor);
        this.seriesChunkReader = new SeriesChunkReaderByTimestampImpl(memSeriesChunk.getSeriesChunkBodyStream()
                , seriesChunkDescriptor.getDataType(),
                seriesChunkDescriptor.getCompressionTypeName());
    }

    @Override
    protected boolean seriesChunkSatisfied(SeriesChunkDescriptor seriesChunkDescriptor) {
        long minTimestamp = seriesChunkDescriptor.getMinTimestamp();
        long maxTimestamp = seriesChunkDescriptor.getMaxTimestamp();
        if (minTimestamp <= currentTimestamp && currentTimestamp <= maxTimestamp) {
            return true;
        }
        return false;
    }
}

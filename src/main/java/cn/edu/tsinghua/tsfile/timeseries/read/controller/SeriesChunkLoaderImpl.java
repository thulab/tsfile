package cn.edu.tsinghua.tsfile.timeseries.read.controller;

import cn.edu.tsinghua.tsfile.common.exception.cache.CacheException;
import cn.edu.tsinghua.tsfile.file.metadata.TimeSeriesChunkMetaData;
import cn.edu.tsinghua.tsfile.timeseries.read.TsFileSequenceReader;
import cn.edu.tsinghua.tsfile.timeseries.read.common.MemSeriesChunk;
import cn.edu.tsinghua.tsfile.timeseries.utils.cache.LRUCache;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Created by zhangjinrui on 2017/12/25.
 */
public class SeriesChunkLoaderImpl implements SeriesChunkLoader {
    private static final int DEFAULT_MEMSERISCHUNK_CACHE_SIZE = 100;
    private TsFileSequenceReader fileSequenceReader;
    private LRUCache<TimeSeriesChunkMetaData, ByteBuffer> seriesChunkBytesCache;

    public SeriesChunkLoaderImpl(TsFileSequenceReader fileSequenceReader) {
        this(fileSequenceReader, DEFAULT_MEMSERISCHUNK_CACHE_SIZE);
    }

    public SeriesChunkLoaderImpl(TsFileSequenceReader fileSequenceReader, int cacheSize) {
        this.fileSequenceReader = fileSequenceReader;
        seriesChunkBytesCache = new LRUCache<TimeSeriesChunkMetaData, ByteBuffer>(cacheSize) {
            @Override
            public void beforeRemove(ByteBuffer object) {
                return;
            }

            @Override
            public ByteBuffer loadObjectByKey(TimeSeriesChunkMetaData key) throws CacheException {
                try {
                    return load(key);
                } catch (IOException e) {
                    throw new CacheException(e);
                }
            }
        };
    }

    public MemSeriesChunk getMemSeriesChunk(TimeSeriesChunkMetaData timeSeriesChunkMetaData) throws IOException {
        try {
            seriesChunkBytesCache.get(timeSeriesChunkMetaData).position(0);
            return new MemSeriesChunk(timeSeriesChunkMetaData, seriesChunkBytesCache.get(timeSeriesChunkMetaData));
        } catch (CacheException e) {
            throw new IOException(e);
        }
    }

    private ByteBuffer load(TimeSeriesChunkMetaData timeSeriesChunkMetaData) throws IOException {
        return fileSequenceReader.readChunkAndHeader(timeSeriesChunkMetaData.getFileOffsetOfCorrespondingData());
    }
}

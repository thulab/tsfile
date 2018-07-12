package cn.edu.tsinghua.tsfile.timeseries.readV2.controller;

import cn.edu.tsinghua.tsfile.common.exception.cache.CacheException;
import cn.edu.tsinghua.tsfile.timeseries.readV2.TsFileSequenceReader;
import cn.edu.tsinghua.tsfile.timeseries.readV2.common.EncodedSeriesChunkDescriptor;
import cn.edu.tsinghua.tsfile.timeseries.readV2.common.MemSeriesChunk;
import cn.edu.tsinghua.tsfile.timeseries.utils.cache.LRUCache;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Created by zhangjinrui on 2017/12/25.
 */
public class SeriesChunkLoaderImpl implements SeriesChunkLoader {
    private static final int DEFAULT_MEMSERISCHUNK_CACHE_SIZE = 100;
    //private ITsRandomAccessFileReader randomAccessFileReader;
    private TsFileSequenceReader fileSequenceReader;
    private LRUCache<EncodedSeriesChunkDescriptor, byte[]> seriesChunkBytesCache;

    public SeriesChunkLoaderImpl(TsFileSequenceReader fileSequenceReader) {
        this(fileSequenceReader, DEFAULT_MEMSERISCHUNK_CACHE_SIZE);
    }

    public SeriesChunkLoaderImpl(TsFileSequenceReader fileSequenceReader, int cacheSize) {
        this.fileSequenceReader = fileSequenceReader;
        seriesChunkBytesCache = new LRUCache<EncodedSeriesChunkDescriptor, byte[]>(cacheSize) {
            @Override
            public void beforeRemove(byte[] object) throws CacheException {
                return;
            }

            @Override
            public byte[] loadObjectByKey(EncodedSeriesChunkDescriptor key) throws CacheException {
                try {
                    return load(key);
                } catch (IOException e) {
                    throw new CacheException(e);
                }
            }
        };
    }

    public MemSeriesChunk getMemSeriesChunk(EncodedSeriesChunkDescriptor encodedSeriesChunkDescriptor) throws IOException {
        try {
            return new MemSeriesChunk(encodedSeriesChunkDescriptor, new ByteArrayInputStream(seriesChunkBytesCache.get(encodedSeriesChunkDescriptor)));
        } catch (CacheException e) {
            throw new IOException(e);
        }
    }

    private byte[] load(EncodedSeriesChunkDescriptor encodedSeriesChunkDescriptor) throws IOException {
        int seriesChunkLength = (int) encodedSeriesChunkDescriptor.getLengthOfBytes();
        ByteBuffer buffer = ByteBuffer.allocate(seriesChunkLength);
        int readLength = fileSequenceReader.readRaw(encodedSeriesChunkDescriptor.getOffsetInFile(), buffer.remaining(), buffer);
        assert readLength == seriesChunkLength;
        assert buffer.array().length == readLength;
        return buffer.array();
    }
}

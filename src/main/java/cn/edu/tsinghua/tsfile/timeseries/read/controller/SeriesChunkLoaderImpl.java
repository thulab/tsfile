package cn.edu.tsinghua.tsfile.timeseries.read.controller;

import cn.edu.tsinghua.tsfile.common.exception.cache.CacheException;
import cn.edu.tsinghua.tsfile.file.metadata.ChunkMetaData;
import cn.edu.tsinghua.tsfile.timeseries.read.TsFileSequenceReader;
import cn.edu.tsinghua.tsfile.timeseries.read.common.MemSeriesChunk;
import cn.edu.tsinghua.tsfile.timeseries.utils.cache.LRUCache;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Read one Chunk and cache it
 *
 * Created by zhangjinrui on 2017/12/25.
 */
public class SeriesChunkLoaderImpl implements SeriesChunkLoader {
    private static final int DEFAULT_MEMSERISCHUNK_CACHE_SIZE = 100;
    private TsFileSequenceReader fileSequenceReader;
    private LRUCache<ChunkMetaData, ByteBuffer> seriesChunkBytesCache;

    public SeriesChunkLoaderImpl(TsFileSequenceReader fileSequenceReader) {
        this(fileSequenceReader, DEFAULT_MEMSERISCHUNK_CACHE_SIZE);
    }

    public SeriesChunkLoaderImpl(TsFileSequenceReader fileSequenceReader, int cacheSize) {
        this.fileSequenceReader = fileSequenceReader;
        seriesChunkBytesCache = new LRUCache<ChunkMetaData, ByteBuffer>(cacheSize) {
            @Override
            public void beforeRemove(ByteBuffer object) {
                return;
            }

            @Override
            public ByteBuffer loadObjectByKey(ChunkMetaData key) throws CacheException {
                try {
                    return load(key);
                } catch (IOException e) {
                    throw new CacheException(e);
                }
            }
        };
    }

    public MemSeriesChunk getMemSeriesChunk(ChunkMetaData chunkMetaData) throws IOException {
        try {
            seriesChunkBytesCache.get(chunkMetaData).position(0);
            return new MemSeriesChunk(chunkMetaData, seriesChunkBytesCache.get(chunkMetaData));
        } catch (CacheException e) {
            throw new IOException(e);
        }
    }

    private ByteBuffer load(ChunkMetaData chunkMetaData) throws IOException {
        return fileSequenceReader.readChunkAndHeader(chunkMetaData.getFileOffsetOfCorrespondingData());
    }
}

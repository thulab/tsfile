package cn.edu.tsinghua.tsfile.timeseries.read.controller;

import cn.edu.tsinghua.tsfile.common.exception.cache.CacheException;
import cn.edu.tsinghua.tsfile.file.metadata.ChunkGroupMetaData;
import cn.edu.tsinghua.tsfile.file.metadata.ChunkMetaData;
import cn.edu.tsinghua.tsfile.file.metadata.TsFileMetaData;
import cn.edu.tsinghua.tsfile.timeseries.read.TsFileSequenceReader;
import cn.edu.tsinghua.tsfile.timeseries.read.common.Path;
import cn.edu.tsinghua.tsfile.timeseries.utils.cache.LRUCache;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by zhangjinrui on 2017/12/25.
 */
public class MetadataQuerierByFileImpl implements MetadataQuerier {
    private static final int SERIESCHUNK_DESCRIPTOR_CACHE_SIZE = 100000;


    private TsFileMetaData fileMetaData;

    private LRUCache<Path, List<ChunkMetaData>> seriesChunkDescriptorCache;


    public MetadataQuerierByFileImpl(TsFileSequenceReader tsFileReader) throws IOException {
        this.fileMetaData=tsFileReader.readFileMetadata();
        seriesChunkDescriptorCache = new LRUCache<Path, List<ChunkMetaData>>(SERIESCHUNK_DESCRIPTOR_CACHE_SIZE) {
            @Override
            public void beforeRemove(List<ChunkMetaData> object) {
            }

            @Override
            public List<ChunkMetaData> loadObjectByKey(Path key) {
                return loadSeriesChunkDescriptor(key);
            }
        };
    }


    @Override
    public List<ChunkMetaData> getSeriesChunkMetaDataList(Path path) throws IOException {
        try {
            return seriesChunkDescriptorCache.get(path);
        } catch (CacheException e) {
            throw new IOException(String.format("Get SeriesChunkDescriptorList for Path[%s] Error.", path), e);
        }
    }

    @Override
    public TsFileMetaData getWholeFileMetadata() {
        return fileMetaData;
    }

    private List<ChunkMetaData> loadSeriesChunkDescriptor(Path path) {
        List<ChunkGroupMetaData> chunkGroupMetaDataList = fileMetaData.getDeltaObject(path.getDeltaObjectToString()).getRowGroups();
        List<ChunkMetaData> chunkMetaDataList = new ArrayList<>();
        for (ChunkGroupMetaData chunkGroupMetaData : chunkGroupMetaDataList) {
            List<ChunkMetaData> chunkMetaDataListInOneRowGroup = chunkGroupMetaData.getChunkMetaDataList();
            for (ChunkMetaData chunkMetaData : chunkMetaDataListInOneRowGroup) {
                if (path.getMeasurementToString().equals(chunkMetaData.getMeasurementUID())) {
                    chunkMetaDataList.add(chunkMetaData);
                }
            }
        }
        return chunkMetaDataList;
    }

}

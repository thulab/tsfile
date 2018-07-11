package cn.edu.tsinghua.tsfile.timeseries.readV2.controller;

import cn.edu.tsinghua.tsfile.common.exception.cache.CacheException;
import cn.edu.tsinghua.tsfile.common.utils.ITsRandomAccessFileReader;
import cn.edu.tsinghua.tsfile.file.metadata.RowGroupMetaData;
import cn.edu.tsinghua.tsfile.file.metadata.TimeSeriesChunkMetaData;
import cn.edu.tsinghua.tsfile.file.metadata.TsDeltaObjectMetadata;
import cn.edu.tsinghua.tsfile.file.metadata.TsFileMetaData;
import cn.edu.tsinghua.tsfile.file.utils.ReadWriteByteStreamUtils;
import cn.edu.tsinghua.tsfile.timeseries.read.support.Path;
import cn.edu.tsinghua.tsfile.timeseries.readV2.common.EncodedSeriesChunkDescriptor;
import cn.edu.tsinghua.tsfile.timeseries.utils.cache.LRUCache;
import cn.edu.tsinghua.tsfile.timeseries.write.io.TsFileIOWriter;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by zhangjinrui on 2017/12/25.
 */
public class MetadataQuerierByFileImpl implements MetadataQuerier {
    private static final int FOOTER_LENGTH = 4;
    private static final int MAGIC_LENGTH = TsFileIOWriter.magicStringBytes.length;
    private static final int ROWGROUP_METADATA_CACHE_SIZE = 1000; //TODO: how to specify this value
    private static final int SERIESCHUNK_DESCRIPTOR_CACHE_SIZE = 100000;

    private ITsRandomAccessFileReader randomAccessFileReader;
    private TsFileMetaData fileMetaData;

    private LRUCache<String, List<RowGroupMetaData>> rowGroupMetadataCache;//TODO: 完全没用啊。。。 都在fileMetadata里 已经在内存中了。。
    private LRUCache<Path, List<EncodedSeriesChunkDescriptor>> seriesChunkDescriptorCache;//TODO: 完全没用啊。。。 都在fileMetadata里 已经在内存中了。。

    public MetadataQuerierByFileImpl(ITsRandomAccessFileReader randomAccessFileReader) throws IOException {
        this.randomAccessFileReader = randomAccessFileReader;
        initFileMetadata();
        rowGroupMetadataCache = new LRUCache<String, List<RowGroupMetaData>>(ROWGROUP_METADATA_CACHE_SIZE) {
            @Override
            public void beforeRemove(List<RowGroupMetaData> object) {
                return;
            }

            @Override
            public List<RowGroupMetaData> loadObjectByKey(String key) throws CacheException {
                try {
                    return loadRowGroupMetadata(key);
                } catch (IOException e) {
                    throw new CacheException(e);
                }
            }
        };

        seriesChunkDescriptorCache = new LRUCache<Path, List<EncodedSeriesChunkDescriptor>>(SERIESCHUNK_DESCRIPTOR_CACHE_SIZE) {
            @Override
            public void beforeRemove(List<EncodedSeriesChunkDescriptor> object) throws CacheException {
                return;
            }

            @Override
            public List<EncodedSeriesChunkDescriptor> loadObjectByKey(Path key) throws CacheException {
                return loadSeriesChunkDescriptor(key);
            }
        };
    }

    private void initFileMetadata() throws IOException {
        long l = randomAccessFileReader.length();
        randomAccessFileReader.seek(l - MAGIC_LENGTH - FOOTER_LENGTH);
        int fileMetaDataLength = randomAccessFileReader.readInt();
        randomAccessFileReader.seek(l - MAGIC_LENGTH - FOOTER_LENGTH - fileMetaDataLength);

        //FIXME can modify the logic
        byte[] buf = new byte[fileMetaDataLength];
        randomAccessFileReader.read(buf, 0, buf.length);
        ByteArrayInputStream metadataInputStream = new ByteArrayInputStream(buf);
        this.fileMetaData = ReadWriteByteStreamUtils.readFileMetaData(metadataInputStream);
    }

    @Override
    public List<EncodedSeriesChunkDescriptor> getSeriesChunkDescriptorList(Path path) throws IOException {
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

    private List<EncodedSeriesChunkDescriptor> loadSeriesChunkDescriptor(Path path) throws CacheException {
        List<RowGroupMetaData> rowGroupMetaDataList = rowGroupMetadataCache.get(path.getDeltaObjectToString());
        List<EncodedSeriesChunkDescriptor> encodedSeriesChunkDescriptorList = new ArrayList<>();
        for (RowGroupMetaData rowGroupMetaData : rowGroupMetaDataList) {
            List<TimeSeriesChunkMetaData> timeSeriesChunkMetaDataListInOneRowGroup = rowGroupMetaData.getTimeSeriesChunkMetaDataList();
            for (TimeSeriesChunkMetaData timeSeriesChunkMetaData : timeSeriesChunkMetaDataListInOneRowGroup) {
                if (path.getMeasurementToString().equals(timeSeriesChunkMetaData.getMeasurementUID())) {
                    encodedSeriesChunkDescriptorList.add(generateSeriesChunkDescriptorByMetadata(timeSeriesChunkMetaData));
                }
            }
        }
        return encodedSeriesChunkDescriptorList;
    }

    private EncodedSeriesChunkDescriptor generateSeriesChunkDescriptorByMetadata(TimeSeriesChunkMetaData timeSeriesChunkMetaData) {
        EncodedSeriesChunkDescriptor encodedSeriesChunkDescriptor = new EncodedSeriesChunkDescriptor(
                timeSeriesChunkMetaData.getFileOffset(),
                timeSeriesChunkMetaData.getTotalByteSize(),
                timeSeriesChunkMetaData.getCompression(),
                timeSeriesChunkMetaData.getDataType(),
                timeSeriesChunkMetaData.getDigest(),
                timeSeriesChunkMetaData.getStartTime(),
                timeSeriesChunkMetaData.getEndTime(),
                timeSeriesChunkMetaData.getNumOfPoints(),
                timeSeriesChunkMetaData.getDataEncoding());
        return encodedSeriesChunkDescriptor;
    }

    private List<RowGroupMetaData> loadRowGroupMetadata(String deltaObjectID) throws IOException {
        TsDeltaObjectMetadata deltaObject = fileMetaData.getDeltaObject(deltaObjectID);
        return deltaObject.getRowGroups();
    }
}

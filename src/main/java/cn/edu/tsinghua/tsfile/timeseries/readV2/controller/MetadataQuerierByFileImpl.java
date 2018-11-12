package cn.edu.tsinghua.tsfile.timeseries.readV2.controller;

import cn.edu.tsinghua.tsfile.common.exception.cache.CacheException;
import cn.edu.tsinghua.tsfile.file.metadata.RowGroupMetaData;
import cn.edu.tsinghua.tsfile.file.metadata.TimeSeriesChunkMetaData;
import cn.edu.tsinghua.tsfile.file.metadata.TsFileMetaData;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.timeseries.readV2.TsFileSequenceReader;
import cn.edu.tsinghua.tsfile.timeseries.readV2.common.EncodedSeriesChunkDescriptor;
import cn.edu.tsinghua.tsfile.timeseries.readV2.common.Path;
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

    private LRUCache<Path, List<EncodedSeriesChunkDescriptor>> seriesChunkDescriptorCache;


    public MetadataQuerierByFileImpl(TsFileSequenceReader tsFileReader) throws IOException {
        this.fileMetaData=tsFileReader.readFileMetadata();
        seriesChunkDescriptorCache = new LRUCache<Path, List<EncodedSeriesChunkDescriptor>>(SERIESCHUNK_DESCRIPTOR_CACHE_SIZE) {
            @Override
            public void beforeRemove(List<EncodedSeriesChunkDescriptor> object) {
            }

            @Override
            public List<EncodedSeriesChunkDescriptor> loadObjectByKey(Path key) {
                return loadSeriesChunkDescriptor(key);
            }
        };
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

    private List<EncodedSeriesChunkDescriptor> loadSeriesChunkDescriptor(Path path) {
        List<RowGroupMetaData> rowGroupMetaDataList = fileMetaData.getDeltaObject(path.getDeltaObjectToString()).getRowGroups();
        List<EncodedSeriesChunkDescriptor> encodedSeriesChunkDescriptorList = new ArrayList<>();
        for (RowGroupMetaData rowGroupMetaData : rowGroupMetaDataList) {
            List<TimeSeriesChunkMetaData> timeSeriesChunkMetaDataListInOneRowGroup = rowGroupMetaData.getTimeSeriesChunkMetaDataList();
            for (TimeSeriesChunkMetaData timeSeriesChunkMetaData : timeSeriesChunkMetaDataListInOneRowGroup) {
                if (path.getMeasurementToString().equals(timeSeriesChunkMetaData.getMeasurementUID())) {
                    encodedSeriesChunkDescriptorList.add(generateSeriesChunkDescriptorByMetadata(timeSeriesChunkMetaData,fileMetaData.getType(timeSeriesChunkMetaData.getMeasurementUID())));
                }
            }
        }
        return encodedSeriesChunkDescriptorList;
    }

    private EncodedSeriesChunkDescriptor generateSeriesChunkDescriptorByMetadata(TimeSeriesChunkMetaData timeSeriesChunkMetaData, TSDataType type) {
        EncodedSeriesChunkDescriptor encodedSeriesChunkDescriptor = new EncodedSeriesChunkDescriptor(
                timeSeriesChunkMetaData.getMeasurementUID(),
                timeSeriesChunkMetaData.getFileOffsetOfCorrespondingData(),
                timeSeriesChunkMetaData.getTotalByteSizeOfPagesOnDisk(),
                type,
                timeSeriesChunkMetaData.getDigest(),
                timeSeriesChunkMetaData.getStartTime(),
                timeSeriesChunkMetaData.getEndTime(),
                timeSeriesChunkMetaData.getNumOfPoints()//,
                );
        return encodedSeriesChunkDescriptor;
    }

}

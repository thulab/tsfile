package cn.edu.tsinghua.tsfile.file.metadata.utils;

import cn.edu.tsinghua.tsfile.common.utils.BytesUtils;
import cn.edu.tsinghua.tsfile.file.metadata.*;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TestHelper {
	private static final String MAX_VALUE = "321";
	private static final String MIN_VALUE = "123";
	private static final String SUM_VALUE = "321123";
	private static final String FIRST_VALUE = "1";
	private static final String LAST_VALUE = "222";

    public static TsFileMetaData createSimpleFileMetaData() {
        TsFileMetaData metaData = new TsFileMetaData(generateDeltaObjectMetadataMap(), new ArrayList<>(), TsFileMetaDataTest.VERSION);
        metaData.addTimeSeriesMetaData(TestHelper.createSimpleTimeSeriesMetaData());
        metaData.addTimeSeriesMetaData(TestHelper.createSimpleTimeSeriesMetaData());
        metaData.setCreatedBy(TsFileMetaDataTest.CREATED_BY);
        metaData.setFirstTimeSeriesMetadataOffset(TsFileMetaDataTest.FIRST_TSMETADATA_OFFSET);
        metaData.setLastTimeSeriesMetadataOffset(TsFileMetaDataTest.LAST_TSMETADATA_OFFSET);
        metaData.setFirstTsDeltaObjectMetadataOffset(TsFileMetaDataTest.FIRST_DOMETADATA_OFFSET);
        metaData.setLastTimeSeriesMetadataOffset(TsFileMetaDataTest.LAST_DOMETADATA_OFFSET);
        return metaData;
    }

    public static Map<String, TsDeltaObjectMetadata> generateDeltaObjectMetadataMap() {
        Map<String, TsDeltaObjectMetadata> deltaObjectMetadataMap = new HashMap<>();
        for(int i = 0;i < 5;i++){
            deltaObjectMetadataMap.put("device_" + i, createSimpleDeltaObjectMetaData());
        }
        return deltaObjectMetadataMap;
    }

    public static TsDeltaObjectMetadata createSimpleDeltaObjectMetaData() {
        TsDeltaObjectMetadata metaData = new TsDeltaObjectMetadata();
        //metaData.setOffset(TsDeltaObjectMetadataTest.OFFSET);
        //metaData.setMetadataBlockSize(TsDeltaObjectMetadataTest.METADATA_BLOCK_SIZE);
        metaData.setStartTime(TsDeltaObjectMetadataTest.START_TIME);
        metaData.setEndTime(TsDeltaObjectMetadataTest.END_TIME);
        metaData.addRowGroupMetaData(TestHelper.createSimpleRowGroupMetaData());
        metaData.addRowGroupMetaData(TestHelper.createSimpleRowGroupMetaData());
        return metaData;
    }

    public static RowGroupMetaData createSimpleRowGroupMetaData() {
    RowGroupMetaData metaData = new RowGroupMetaData(RowGroupMetaDataTest.DELTA_OBJECT_UID,
            RowGroupMetaDataTest.TOTAL_BYTE_SIZE, 12, new ArrayList<>());
    metaData.addTimeSeriesChunkMetaData(TestHelper.createSimpleTimeSeriesChunkMetaData());
    metaData.addTimeSeriesChunkMetaData(TestHelper.createSimpleTimeSeriesChunkMetaData());
    //metaData.setMetadataOffset(RowGroupMetaDataTest.METADATA_OFFSET);
    //metaData.setMetadataSize(RowGroupMetaDataTest.METADATA_SIZE);
    return metaData;
    }

    public static TimeSeriesChunkMetaData createSimpleTimeSeriesChunkMetaData() {
    TimeSeriesChunkMetaData metaData =
        new TimeSeriesChunkMetaData(TimeSeriesChunkMetaDataTest.MEASUREMENT_UID, TimeSeriesChunkMetaDataTest.FILE_OFFSET,
            //TimeSeriesChunkMetaDataTest.COMPRESSION_TYPE, TimeSeriesChunkMetaDataTest.DATA_TYPE,
            TimeSeriesChunkMetaDataTest.START_TIME, TimeSeriesChunkMetaDataTest.END_TIME//, TimeSeriesChunkMetaDataTest.ENCODING_TYPE
        );
    //metaData.setTsDigestOffset(TimeSeriesChunkMetaDataTest.DIGEST_OFFSET);
    metaData.setNumOfPoints(TimeSeriesChunkMetaDataTest.NUM_OF_POINTS);
    metaData.setTotalByteSizeOfPagesOnDisk(TimeSeriesChunkMetaDataTest.TOTAL_BYTE_SIZE);
    metaData.setDigest(new TsDigest());
    return metaData;
    }

    public static TimeSeriesMetadata createSimpleTimeSeriesMetaData() {
    TimeSeriesMetadata timeSeries = new TimeSeriesMetadata(TimeSeriesMetadataTest.measurementUID,
        TSDataType.INT64);
    return timeSeries;
    }

    public static TsDigest createSimpleTsDigest() {
      TsDigest digest = new TsDigest();
      digest.addStatistics("max", ByteBuffer.wrap(BytesUtils.StringToBytes(MAX_VALUE)));
      digest.addStatistics("min", ByteBuffer.wrap(BytesUtils.StringToBytes(MIN_VALUE)));
      digest.addStatistics("sum", ByteBuffer.wrap(BytesUtils.StringToBytes(SUM_VALUE)));
      digest.addStatistics("first", ByteBuffer.wrap(BytesUtils.StringToBytes(FIRST_VALUE)));
      digest.addStatistics("last", ByteBuffer.wrap(BytesUtils.StringToBytes(LAST_VALUE)));
      return digest;
    }

    public static List<String> getJSONArray() {
    List<String> jsonMetaData = new ArrayList<String>();
    jsonMetaData.add("fsdfsfsd");
    jsonMetaData.add("424fd");
    return jsonMetaData;
    }
}

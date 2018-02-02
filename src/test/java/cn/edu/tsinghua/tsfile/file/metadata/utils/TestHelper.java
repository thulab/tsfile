package cn.edu.tsinghua.tsfile.file.metadata.utils;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import cn.edu.tsinghua.tsfile.file.header.DataPageHeader;
import cn.edu.tsinghua.tsfile.file.header.PageHeader;
import cn.edu.tsinghua.tsfile.file.header.PageType;
import cn.edu.tsinghua.tsfile.file.metadata.enums.*;
import cn.edu.tsinghua.tsfile.common.utils.BytesUtils;
import cn.edu.tsinghua.tsfile.file.metadata.RowGroupMetaData;
import cn.edu.tsinghua.tsfile.file.metadata.RowGroupMetaDataTest;
import cn.edu.tsinghua.tsfile.file.metadata.TInTimeSeriesChunkMetaData;
import cn.edu.tsinghua.tsfile.file.metadata.TInTimeSeriesChunkMetaDataTest;
import cn.edu.tsinghua.tsfile.file.metadata.TsDigest;
import cn.edu.tsinghua.tsfile.file.metadata.TimeSeriesChunkMetaData;
import cn.edu.tsinghua.tsfile.file.metadata.TimeSeriesChunkMetaDataTest;
import cn.edu.tsinghua.tsfile.file.metadata.TimeSeriesMetadata;
import cn.edu.tsinghua.tsfile.file.metadata.TimeSeriesMetadataTest;
import cn.edu.tsinghua.tsfile.file.metadata.VInTimeSeriesChunkMetaData;
import cn.edu.tsinghua.tsfile.file.metadata.VInTimeSeriesChunkMetaDataTest;

public class TestHelper {
	private static final String MAX_VALUE = "321";
	private static final String MIN_VALUE = "123";
	private static final String SUM_VALUE = "321123";
	private static final String FIRST_VALUE = "1";
	private static final String LAST_VALUE = "222";

  public static RowGroupMetaData createSimpleRowGroupMetaDataInTSF()
      throws UnsupportedEncodingException {
    RowGroupMetaData metaData = new RowGroupMetaData(RowGroupMetaDataTest.DELTA_OBJECT_UID,
        RowGroupMetaDataTest.MAX_NUM_ROWS, RowGroupMetaDataTest.TOTAL_BYTE_SIZE, new ArrayList<>(),
        RowGroupMetaDataTest.DELTA_OBJECT_TYPE);
    metaData.setPath(RowGroupMetaDataTest.FILE_PATH);
    metaData.addTimeSeriesChunkMetaData(TestHelper.createSimpleTimeSeriesChunkMetaDataInTSF());
    metaData.addTimeSeriesChunkMetaData(TestHelper.createSimpleTimeSeriesChunkMetaDataInTSF());
    return metaData;
  }

  public static TimeSeriesChunkMetaData createSimpleTimeSeriesChunkMetaDataInTSF()
      throws UnsupportedEncodingException {

    TimeSeriesChunkMetaData metaData =
        new TimeSeriesChunkMetaData(TimeSeriesChunkMetaDataTest.MEASUREMENT_UID, TSChunkType.TIME,
            TimeSeriesChunkMetaDataTest.FILE_OFFSET, CompressionTypeName.GZIP);
    metaData.setNumRows(TimeSeriesChunkMetaDataTest.MAX_NUM_ROWS);
    metaData.setTotalByteSize(TimeSeriesChunkMetaDataTest.TOTAL_BYTE_SIZE);
    metaData.setJsonMetaData(TestHelper.getJSONArray());
    metaData.setDataPageOffset(TimeSeriesChunkMetaDataTest.DATA_PAGE_OFFSET);
    metaData.setDictionaryPageOffset(TimeSeriesChunkMetaDataTest.DICTIONARY_PAGE_OFFSET);
    metaData.setIndexPageOffset(TimeSeriesChunkMetaDataTest.INDEX_PAGE_OFFSET);
    metaData.setTInTimeSeriesChunkMetaData(TestHelper.createT2inTSF(TSDataType.BOOLEAN,
        TSFreqType.IRREGULAR_FREQ, null, TInTimeSeriesChunkMetaDataTest.startTime, TInTimeSeriesChunkMetaDataTest.endTime));
    metaData.setVInTimeSeriesChunkMetaData(TestHelper.createSimpleV2InTSF(TSDataType.BOOLEAN, new TsDigest()));
    return metaData;
  }

  public static TimeSeriesMetadata createSimpleTimeSeriesInTSF() {
    TimeSeriesMetadata timeSeries = new TimeSeriesMetadata(TimeSeriesMetadataTest.measurementUID,
        TSDataType.FIXED_LEN_BYTE_ARRAY);
    timeSeries.setFreqType(TSFreqType.SINGLE_FREQ);
    timeSeries.setTypeLength(TimeSeriesMetadataTest.typeLength);
    List<Integer> frequencies = new ArrayList<Integer>();
    frequencies.add(132);
    frequencies.add(432);
    frequencies.add(35435);
    timeSeries.setFrequencies(frequencies);
    List<String> dataValues = new ArrayList<String>();
    dataValues.add("A");
    dataValues.add("B");
    dataValues.add("C");
    dataValues.add("D");
    timeSeries.setEnumValues(dataValues);
    return timeSeries;
  }

  public static TInTimeSeriesChunkMetaData createT1inTSF(TSDataType dataType, long startTime,
                                                         long endTime) {
    TInTimeSeriesChunkMetaData metaData =
        new TInTimeSeriesChunkMetaData(dataType, startTime, endTime);
    return metaData;
  }

  public static TInTimeSeriesChunkMetaData createT2inTSF(TSDataType dataType, TSFreqType freqType,
      List<Integer> frequencies, long startTime, long endTime) {
    TInTimeSeriesChunkMetaData metaData =
        new TInTimeSeriesChunkMetaData(dataType, startTime, endTime);
    metaData.setFreqType(freqType);
    metaData.setFrequencies(frequencies);

    List<String> dataValues = new ArrayList<String>();
    dataValues.add("A");
    dataValues.add("B");
    dataValues.add("C");
    dataValues.add("D");
    metaData.setEnumValues(dataValues);
    return metaData;

  }

  public static List<TInTimeSeriesChunkMetaData> generateTSeriesChunkMetaDataListInTSF() {
    ArrayList<Integer> frequencies1 = new ArrayList<Integer>();

    ArrayList<Integer> frequencies2 = new ArrayList<Integer>();
    frequencies2.add(132);
    frequencies2.add(432);
    frequencies2.add(35435);
    List<TInTimeSeriesChunkMetaData> list = new ArrayList<TInTimeSeriesChunkMetaData>();
    for (TSDataType dataType : TSDataType.values()) {
      list.add(createT1inTSF(dataType, TInTimeSeriesChunkMetaDataTest.startTime,
          TInTimeSeriesChunkMetaDataTest.endTime));

      for (TSFreqType freqType : TSFreqType.values()) {
        list.add(createT2inTSF(dataType, freqType, null, TInTimeSeriesChunkMetaDataTest.startTime,
            TInTimeSeriesChunkMetaDataTest.endTime));
        list.add(createT2inTSF(dataType, freqType, frequencies1,
            TInTimeSeriesChunkMetaDataTest.startTime, TInTimeSeriesChunkMetaDataTest.endTime));
        list.add(createT2inTSF(dataType, freqType, frequencies2,
            TInTimeSeriesChunkMetaDataTest.startTime, TInTimeSeriesChunkMetaDataTest.endTime));
      }
    }
    return list;
  }

  public static List<VInTimeSeriesChunkMetaData> generateVSeriesChunkMetaDataListInTSF()
      throws UnsupportedEncodingException {
    List<VInTimeSeriesChunkMetaData> list = new ArrayList<VInTimeSeriesChunkMetaData>();
    for (TSDataType dataType : TSDataType.values()) {
      list.add(TestHelper.createSimpleV1InTSF(dataType, null));
      list.add(TestHelper.createSimpleV1InTSF(dataType, new TsDigest()));
      list.add(TestHelper.createSimpleV2InTSF(dataType, createSimpleTsDigest()));
    }
    return list;
  }

  public static VInTimeSeriesChunkMetaData createSimpleV2InTSF(TSDataType dataType, TsDigest digest) throws UnsupportedEncodingException {
    VInTimeSeriesChunkMetaData metaData = new VInTimeSeriesChunkMetaData(dataType);
    metaData.setMaxError(VInTimeSeriesChunkMetaDataTest.MAX_ERROR);
    metaData.setDigest(digest);

    List<String> dataValues = new ArrayList<String>();
    dataValues.add("A");
    dataValues.add("B");
    dataValues.add("C");
    dataValues.add("D");
    metaData.setEnumValues(dataValues);
    return metaData;
  }

  public static VInTimeSeriesChunkMetaData createSimpleV1InTSF(TSDataType dataType, TsDigest digest)
      throws UnsupportedEncodingException {
    VInTimeSeriesChunkMetaData metaData = new VInTimeSeriesChunkMetaData(dataType);
    metaData.setMaxError(VInTimeSeriesChunkMetaDataTest.MAX_ERROR);
    metaData.setDigest(digest);
    return metaData;
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

  public static DataPageHeader createSimpleDataPageHeader(){
    TsDigest digest = createSimpleTsDigest();
    return new DataPageHeader(100, 100, TSEncoding.PLAIN, digest, false, 100, 1);
  }

  public static PageHeader createSimplePageHeader(){
    DataPageHeader dataPageHeader = createSimpleDataPageHeader();
    return new PageHeader(PageType.DATA_PAGE, 100, 100, 0, dataPageHeader, null, null);
  }

  public static List<String> getJSONArray() {
    List<String> jsonMetaData = new ArrayList<String>();
    jsonMetaData.add("fsdfsfsd");
    jsonMetaData.add("424fd");
    return jsonMetaData;
  }
}

package cn.edu.tsinghua.tsfile.file.metadata.utils;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import cn.edu.tsinghua.tsfile.file.metadata.enums.TSFreqType;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.common.utils.BytesUtils;
import cn.edu.tsinghua.tsfile.file.metadata.RowGroupMetaData;
import cn.edu.tsinghua.tsfile.file.metadata.RowGroupMetaDataTest;
import cn.edu.tsinghua.tsfile.file.metadata.TInTimeSeriesChunkMetaDataTest;
import cn.edu.tsinghua.tsfile.file.metadata.TsDigest;
import cn.edu.tsinghua.tsfile.file.metadata.TimeSeriesChunkMetaData;
import cn.edu.tsinghua.tsfile.file.metadata.TimeSeriesChunkMetaDataTest;
import cn.edu.tsinghua.tsfile.file.metadata.TimeSeriesMetadata;
import cn.edu.tsinghua.tsfile.file.metadata.TimeSeriesMetadataTest;
import cn.edu.tsinghua.tsfile.file.metadata.VInTimeSeriesChunkMetaDataTest;
import cn.edu.tsinghua.tsfile.format.CompressionType;
import cn.edu.tsinghua.tsfile.format.DataType;
import cn.edu.tsinghua.tsfile.format.Digest;
import cn.edu.tsinghua.tsfile.format.FreqType;
import cn.edu.tsinghua.tsfile.format.TimeInTimeSeriesChunkMetaData;
import cn.edu.tsinghua.tsfile.format.TimeSeries;
import cn.edu.tsinghua.tsfile.format.TimeSeriesChunkType;
import cn.edu.tsinghua.tsfile.format.ValueInTimeSeriesChunkMetaData;

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

  public static cn.edu.tsinghua.tsfile.format.RowGroupMetaData createSimpleRowGroupMetaDataInThrift()
      throws UnsupportedEncodingException {
    cn.edu.tsinghua.tsfile.format.RowGroupMetaData rowGroupMetaData =
        new cn.edu.tsinghua.tsfile.format.RowGroupMetaData(new ArrayList<>(),
            RowGroupMetaDataTest.DELTA_OBJECT_UID, RowGroupMetaDataTest.TOTAL_BYTE_SIZE,
            RowGroupMetaDataTest.MAX_NUM_ROWS, RowGroupMetaDataTest.DELTA_OBJECT_TYPE);
    rowGroupMetaData.setFile_path(RowGroupMetaDataTest.FILE_PATH);
    rowGroupMetaData.setTsc_metadata(new ArrayList<>());
    rowGroupMetaData.getTsc_metadata()
        .add(TestHelper.createSimpleTimeSeriesChunkMetaDataInThrift());
    rowGroupMetaData.getTsc_metadata()
        .add(TestHelper.createSimpleTimeSeriesChunkMetaDataInThrift());
    return rowGroupMetaData;
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

  public static cn.edu.tsinghua.tsfile.format.TimeSeriesChunkMetaData createSimpleTimeSeriesChunkMetaDataInThrift()
      throws UnsupportedEncodingException {
    cn.edu.tsinghua.tsfile.format.TimeSeriesChunkMetaData metaData =
        new cn.edu.tsinghua.tsfile.format.TimeSeriesChunkMetaData(
            TimeSeriesChunkMetaDataTest.MEASUREMENT_UID, TimeSeriesChunkType.VALUE,
            TimeSeriesChunkMetaDataTest.FILE_OFFSET, CompressionType.LZO);
    metaData.setNum_rows(TimeSeriesChunkMetaDataTest.MAX_NUM_ROWS);
    metaData.setTotal_byte_size(TimeSeriesChunkMetaDataTest.TOTAL_BYTE_SIZE);
    metaData.setJson_metadata(TestHelper.getJSONArray());
    metaData.setData_page_offset(TimeSeriesChunkMetaDataTest.DATA_PAGE_OFFSET);
    metaData.setDictionary_page_offset(TimeSeriesChunkMetaDataTest.DICTIONARY_PAGE_OFFSET);
    metaData.setIndex_page_offset(TimeSeriesChunkMetaDataTest.INDEX_PAGE_OFFSET);
    metaData.setTime_tsc(TestHelper.createT2inThrift(DataType.BOOLEAN, FreqType.IRREGULAR_FREQ,
        null, TInTimeSeriesChunkMetaDataTest.startTime, TInTimeSeriesChunkMetaDataTest.endTime));
    metaData.setValue_tsc(TestHelper.createSimpleV2InThrift(DataType.BOOLEAN, createSimpleDigest()));
    return metaData;
  }

  public static TimeSeriesMetadata createSimpleTimeSeriesInTSF() {
    TimeSeriesMetadata timeSeries = new TimeSeriesMetadata(TimeSeriesMetadataTest.measurementUID,
        TSDataType.INT64);
    return timeSeries;
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

  public static ValueInTimeSeriesChunkMetaData createSimpleV1InThrift(DataType dataType,
      Digest digest) throws UnsupportedEncodingException {
    ValueInTimeSeriesChunkMetaData metaData = new ValueInTimeSeriesChunkMetaData(dataType);
    metaData.setMax_error(VInTimeSeriesChunkMetaDataTest.MAX_ERROR);
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

  public static List<String> getJSONArray() {
    List<String> jsonMetaData = new ArrayList<String>();
    jsonMetaData.add("fsdfsfsd");
    jsonMetaData.add("424fd");
    return jsonMetaData;
  }
}

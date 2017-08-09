package cn.edu.tsinghua.tsfile.file.metadata.utils;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import cn.edu.tsinghua.tsfile.file.metadata.enums.TSChunkType;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSFreqType;
import cn.edu.tsinghua.tsfile.file.metadata.enums.CompressionTypeName;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.file.metadata.RowGroupMetaData;
import cn.edu.tsinghua.tsfile.file.metadata.RowGroupMetaDataTest;
import cn.edu.tsinghua.tsfile.file.metadata.TInTimeSeriesChunkMetaData;
import cn.edu.tsinghua.tsfile.file.metadata.TInTimeSeriesChunkMetaDataTest;
import cn.edu.tsinghua.tsfile.file.metadata.TSDigest;
import cn.edu.tsinghua.tsfile.file.metadata.TimeSeriesChunkMetaData;
import cn.edu.tsinghua.tsfile.file.metadata.TimeSeriesChunkMetaDataTest;
import cn.edu.tsinghua.tsfile.file.metadata.TimeSeriesMetadata;
import cn.edu.tsinghua.tsfile.file.metadata.TimeSeriesMetadataTest;
import cn.edu.tsinghua.tsfile.file.metadata.VInTimeSeriesChunkMetaData;
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
    metaData.setVInTimeSeriesChunkMetaData(
        TestHelper.createSimpleV2InTSF(TSDataType.BOOLEAN, new TSDigest(), VInTimeSeriesChunkMetaDataTest.maxString, VInTimeSeriesChunkMetaDataTest.minString));
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
    metaData.setValue_tsc(
        TestHelper.createSimpleV2InThrift(DataType.BOOLEAN, new Digest(), VInTimeSeriesChunkMetaDataTest.maxString, VInTimeSeriesChunkMetaDataTest.minString));
    return metaData;
  }

  public static TimeSeriesMetadata createSimpleTimeSeriesInTSF() {
    TimeSeriesMetadata timeSeries = new TimeSeriesMetadata(TimeSeriesMetadataTest.measurementUID,
        TSDataType.FIXED_LEN_BYTE_ARRAY, TimeSeriesMetadataTest.deltaObjectType);
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

  public static TimeSeries createSimpleTimeSeriesInThrift() {
    TimeSeries timeSeries = new TimeSeries(TimeSeriesMetadataTest.measurementUID,
        DataType.TEXT, TimeSeriesMetadataTest.deltaObjectType);
    timeSeries.setFreq_type(FreqType.MULTI_FREQ);
    timeSeries.setType_length(TimeSeriesMetadataTest.typeLength);
    List<Integer> frequencies = new ArrayList<Integer>();
    frequencies.add(132);
    frequencies.add(432);
    frequencies.add(35435);
    timeSeries.setFrequencies(frequencies);
    List<String> dataValues = new ArrayList<String>();
    dataValues.add("Q");
    dataValues.add("W");
    dataValues.add("E");
    dataValues.add("R");
    timeSeries.setEnum_values(dataValues);
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

  public static List<TimeInTimeSeriesChunkMetaData> generateTimeInTimeSeriesChunkMetaDataInThrift() {
    ArrayList<Integer> frequencies1 = new ArrayList<Integer>();

    ArrayList<Integer> frequencies2 = new ArrayList<Integer>();
    frequencies2.add(132);
    frequencies2.add(432);
    frequencies2.add(35435);
    List<TimeInTimeSeriesChunkMetaData> list = new ArrayList<TimeInTimeSeriesChunkMetaData>();
    for (DataType dataType : DataType.values()) {
      list.add(TestHelper.createT1inThrift(dataType, TInTimeSeriesChunkMetaDataTest.startTime,
          TInTimeSeriesChunkMetaDataTest.endTime));

      for (FreqType freqType : FreqType.values()) {
        list.add(TestHelper.createT2inThrift(dataType, freqType, null,
            TInTimeSeriesChunkMetaDataTest.startTime, TInTimeSeriesChunkMetaDataTest.endTime));
        list.add(TestHelper.createT2inThrift(dataType, freqType, frequencies1,
            TInTimeSeriesChunkMetaDataTest.startTime, TInTimeSeriesChunkMetaDataTest.endTime));
        list.add(TestHelper.createT2inThrift(dataType, freqType, frequencies2,
            TInTimeSeriesChunkMetaDataTest.startTime, TInTimeSeriesChunkMetaDataTest.endTime));
      }
    }
    return list;
  }

  public static TimeInTimeSeriesChunkMetaData createT1inThrift(DataType dataType, long startTime,
      long endTime) {
    TimeInTimeSeriesChunkMetaData metaData =
        new TimeInTimeSeriesChunkMetaData(dataType, startTime, endTime);
    return metaData;
  }

  public static TimeInTimeSeriesChunkMetaData createT2inThrift(DataType dataType, FreqType freqType,
      List<Integer> frequencies, long startTime, long endTime) {
    TimeInTimeSeriesChunkMetaData metaData =
        new TimeInTimeSeriesChunkMetaData(dataType, startTime, endTime);
    metaData.setFreq_type(freqType);
    metaData.setFrequencies(frequencies);
    List<String> dataValues = new ArrayList<String>();
    dataValues.add("Q");
    dataValues.add("W");
    dataValues.add("E");
    dataValues.add("R");
    metaData.setEnum_values(dataValues);
    return metaData;
  }

  public static List<VInTimeSeriesChunkMetaData> generateVSeriesChunkMetaDataListInTSF()
      throws UnsupportedEncodingException {
    List<VInTimeSeriesChunkMetaData> list = new ArrayList<VInTimeSeriesChunkMetaData>();
    for (TSDataType dataType : TSDataType.values()) {
      list.add(TestHelper.createSimpleV1InTSF(dataType, null));
      list.add(TestHelper.createSimpleV1InTSF(dataType, new TSDigest()));
      list.add(TestHelper.createSimpleV2InTSF(dataType, new TSDigest(),
          VInTimeSeriesChunkMetaDataTest.maxString, VInTimeSeriesChunkMetaDataTest.minString));
    }
    return list;
  }

  public static List<ValueInTimeSeriesChunkMetaData> generateValueInTimeSeriesChunkMetaDataInThrift()
      throws UnsupportedEncodingException {
    List<ValueInTimeSeriesChunkMetaData> list = new ArrayList<ValueInTimeSeriesChunkMetaData>();
    for (DataType dataType : DataType.values()) {
      list.add(TestHelper.createSimpleV1InThrift(dataType, null));
      list.add(TestHelper.createSimpleV1InThrift(dataType, new Digest()));
      list.add(TestHelper.createSimpleV2InThrift(dataType, new Digest(),
          VInTimeSeriesChunkMetaDataTest.maxString, VInTimeSeriesChunkMetaDataTest.minString));
    }
    return list;
  }

  public static ValueInTimeSeriesChunkMetaData createSimpleV2InThrift(DataType dataType,
      Digest digest, String maxString, String minString) throws UnsupportedEncodingException {
    ByteBuffer max = ByteBuffer.wrap(maxString.getBytes("UTF-8"));
    ByteBuffer min = ByteBuffer.wrap(minString.getBytes("UTF-8"));

    ValueInTimeSeriesChunkMetaData metaData = new ValueInTimeSeriesChunkMetaData(dataType);
    metaData.setMax_error(VInTimeSeriesChunkMetaDataTest.MAX_ERROR);
    digest.max = max;
    digest.min = min;
    metaData.setDigest(digest);

    List<String> dataValues = new ArrayList<String>();
    dataValues.add("Q");
    dataValues.add("W");
    dataValues.add("E");
    dataValues.add("R");
    metaData.setEnum_values(dataValues);
    return metaData;
  }

  public static ValueInTimeSeriesChunkMetaData createSimpleV1InThrift(DataType dataType,
      Digest digest) throws UnsupportedEncodingException {
    ValueInTimeSeriesChunkMetaData metaData = new ValueInTimeSeriesChunkMetaData(dataType);
    metaData.setMax_error(VInTimeSeriesChunkMetaDataTest.MAX_ERROR);
    metaData.setDigest(digest);
    return metaData;
  }

  public static VInTimeSeriesChunkMetaData createSimpleV2InTSF(TSDataType dataType, TSDigest digest,
      String maxString, String minString) throws UnsupportedEncodingException {
    VInTimeSeriesChunkMetaData metaData = new VInTimeSeriesChunkMetaData(dataType);
    metaData.setMaxError(VInTimeSeriesChunkMetaDataTest.MAX_ERROR);
    ByteBuffer max = ByteBuffer.wrap(maxString.getBytes("UTF-8"));
    ByteBuffer min = ByteBuffer.wrap(minString.getBytes("UTF-8"));
    digest.max = max;
    digest.min = min;
    metaData.setDigest(digest);

    List<String> dataValues = new ArrayList<String>();
    dataValues.add("A");
    dataValues.add("B");
    dataValues.add("C");
    dataValues.add("D");
    metaData.setEnumValues(dataValues);
    return metaData;
  }

  public static VInTimeSeriesChunkMetaData createSimpleV1InTSF(TSDataType dataType, TSDigest digest)
      throws UnsupportedEncodingException {
    VInTimeSeriesChunkMetaData metaData = new VInTimeSeriesChunkMetaData(dataType);
    metaData.setMaxError(VInTimeSeriesChunkMetaDataTest.MAX_ERROR);
    metaData.setDigest(digest);
    return metaData;
  }

  public static List<String> getJSONArray() {
    List<String> jsonMetaData = new ArrayList<String>();
    jsonMetaData.add("fsdfsfsd");
    jsonMetaData.add("424fd");
    return jsonMetaData;
  }
}

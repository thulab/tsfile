package cn.edu.tsinghua.tsfile.file.metadata;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;

import cn.edu.tsinghua.tsfile.file.metadata.enums.CompressionTypeName;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSChunkType;
import cn.edu.tsinghua.tsfile.file.metadata.utils.TestHelper;
import cn.edu.tsinghua.tsfile.file.metadata.utils.Utils;
import cn.edu.tsinghua.tsfile.file.utils.ReadWriteThriftFormatUtils;
import cn.edu.tsinghua.tsfile.format.CompressionType;
import cn.edu.tsinghua.tsfile.common.utils.RandomAccessOutputStream;
import cn.edu.tsinghua.tsfile.format.ValueInTimeSeriesChunkMetaData;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import cn.edu.tsinghua.tsfile.format.TimeInTimeSeriesChunkMetaData;
import cn.edu.tsinghua.tsfile.format.TimeSeriesChunkType;

public class TimeSeriesChunkMetaDataTest {

  public static final String MEASUREMENT_UID = "sensor231";
  public static final long FILE_OFFSET = 2313424242L;
  public static final long MAX_NUM_ROWS = 423432425L;
  public static final long TOTAL_BYTE_SIZE = 432453453L;
  public static final long DATA_PAGE_OFFSET = 42354334L;
  public static final long DICTIONARY_PAGE_OFFSET = 23434543L;
  public static final long INDEX_PAGE_OFFSET = 34243453L;
  final String PATH = "target/outputTimeSeriesChunk.ksn";

  @Before
  public void setUp() throws Exception {}

  @After
  public void tearDown() throws Exception {
    File file = new File(PATH);
    if (file.exists())
      file.delete();
  }

  @Test
  public void testWriteIntoFile() throws IOException {
    TimeSeriesChunkMetaData metaData = TestHelper.createSimpleTimeSeriesChunkMetaDataInTSF();
    File file = new File(PATH);
    if (file.exists())
      file.delete();
    FileOutputStream fos = new FileOutputStream(file);
    RandomAccessOutputStream out = new RandomAccessOutputStream(file, "rw");
    ReadWriteThriftFormatUtils.write(metaData.convertToThrift(), out.getOutputStream());

    out.close();
    fos.close();

    FileInputStream fis = new FileInputStream(new File(PATH));
    Utils.isTimeSeriesChunkMetaDataEqual(metaData, metaData.convertToThrift());
    Utils.isTimeSeriesChunkMetaDataEqual(metaData,
    		ReadWriteThriftFormatUtils.read(fis, new cn.edu.tsinghua.tsfile.format.TimeSeriesChunkMetaData()));
  }

  @Test
  public void testConvertToThrift() throws UnsupportedEncodingException {
    for (CompressionTypeName compressionTypeName : CompressionTypeName.values()) {
      for (TSChunkType chunkType : TSChunkType.values()) {
        TimeSeriesChunkMetaData metaData = new TimeSeriesChunkMetaData(MEASUREMENT_UID, chunkType,
            FILE_OFFSET, compressionTypeName);
        Utils.isTimeSeriesChunkMetaDataEqual(metaData, metaData.convertToThrift());

        metaData.setNumRows(MAX_NUM_ROWS);
        metaData.setTotalByteSize(TOTAL_BYTE_SIZE);

        metaData.setJsonMetaData(TestHelper.getJSONArray());

        metaData.setDataPageOffset(DATA_PAGE_OFFSET);
        metaData.setDictionaryPageOffset(DICTIONARY_PAGE_OFFSET);
        metaData.setIndexPageOffset(INDEX_PAGE_OFFSET);
        Utils.isTimeSeriesChunkMetaDataEqual(metaData, metaData.convertToThrift());
        for (TInTimeSeriesChunkMetaData tSeriesMetaData : TestHelper
            .generateTSeriesChunkMetaDataListInTSF()) {
          metaData.setTInTimeSeriesChunkMetaData(tSeriesMetaData);
          Utils.isTimeSeriesChunkMetaDataEqual(metaData, metaData.convertToThrift());
          for (VInTimeSeriesChunkMetaData vSeriesMetaData : TestHelper
              .generateVSeriesChunkMetaDataListInTSF()) {
            metaData.setVInTimeSeriesChunkMetaData(vSeriesMetaData);
            Utils.isTimeSeriesChunkMetaDataEqual(metaData, metaData.convertToThrift());
          }
        }
      }
    }
  }

  @Test
  public void testConvertToTSF() throws UnsupportedEncodingException {
    for (CompressionType compressionType : CompressionType.values()) {
      for (TimeSeriesChunkType chunkType : TimeSeriesChunkType.values()) {
        TimeSeriesChunkMetaData metaData = new TimeSeriesChunkMetaData();
        cn.edu.tsinghua.tsfile.format.TimeSeriesChunkMetaData timeSeriesChunkMetaData =
            new cn.edu.tsinghua.tsfile.format.TimeSeriesChunkMetaData(MEASUREMENT_UID, chunkType,
                FILE_OFFSET, compressionType);
        metaData.convertToTSF(timeSeriesChunkMetaData);
        Utils.isTimeSeriesChunkMetaDataEqual(metaData, timeSeriesChunkMetaData);

        timeSeriesChunkMetaData.setNum_rows(MAX_NUM_ROWS);
        timeSeriesChunkMetaData.setTotal_byte_size(TOTAL_BYTE_SIZE);

        timeSeriesChunkMetaData.setJson_metadata(TestHelper.getJSONArray());
        timeSeriesChunkMetaData.setData_page_offset(DATA_PAGE_OFFSET);
        timeSeriesChunkMetaData.setDictionary_page_offset(DICTIONARY_PAGE_OFFSET);
        timeSeriesChunkMetaData.setIndex_page_offset(INDEX_PAGE_OFFSET);

        metaData.convertToTSF(timeSeriesChunkMetaData);
        Utils.isTimeSeriesChunkMetaDataEqual(metaData, timeSeriesChunkMetaData);

        for (TimeInTimeSeriesChunkMetaData tSeriesChunkMetaData : TestHelper
            .generateTimeInTimeSeriesChunkMetaDataInThrift()) {
          timeSeriesChunkMetaData.setTime_tsc(tSeriesChunkMetaData);
          metaData.convertToTSF(timeSeriesChunkMetaData);
          Utils.isTimeSeriesChunkMetaDataEqual(metaData, timeSeriesChunkMetaData);
          for (ValueInTimeSeriesChunkMetaData vSeriesChunkMetaData : TestHelper
              .generateValueInTimeSeriesChunkMetaDataInThrift()) {
            timeSeriesChunkMetaData.setValue_tsc(vSeriesChunkMetaData);
            metaData.convertToTSF(timeSeriesChunkMetaData);
            Utils.isTimeSeriesChunkMetaDataEqual(metaData, timeSeriesChunkMetaData);
          }
        }
      }
    }
  }
}

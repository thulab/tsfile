package cn.edu.tsinghua.tsfile.file.metadata;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;

import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.file.metadata.utils.TestHelper;
import cn.edu.tsinghua.tsfile.format.DataType;
import cn.edu.tsinghua.tsfile.format.Digest;
import cn.edu.tsinghua.tsfile.common.utils.RandomAccessOutputStream;
import cn.edu.tsinghua.tsfile.file.metadata.utils.Utils;
import cn.edu.tsinghua.tsfile.file.utils.ReadWriteThriftFormatUtils;
import cn.edu.tsinghua.tsfile.format.ValueInTimeSeriesChunkMetaData;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class VInTimeSeriesChunkMetaDataTest {
  private VInTimeSeriesChunkMetaData metaData;
  public static final int MAX_ERROR = 1232;
  public static final String maxString = "3244324";
  public static final String minString = "fddsfsfgd";
  final String PATH = "target/outputV.ksn";
  @Before
  public void setUp() throws Exception {
    metaData = new VInTimeSeriesChunkMetaData();
  }

  @After
  public void tearDown() throws Exception {
    File file = new File(PATH);
    if (file.exists())
      file.delete();
  }

  @Test
  public void testWriteIntoFile() throws IOException {
    VInTimeSeriesChunkMetaData metaData =
        TestHelper.createSimpleV2InTSF(TSDataType.TEXT, new TSDigest(), maxString, minString);
    
    File file = new File(PATH);
    if (file.exists())
      file.delete();
    FileOutputStream fos = new FileOutputStream(file);
    RandomAccessOutputStream out = new RandomAccessOutputStream(file, "rw");
    ReadWriteThriftFormatUtils.write(metaData.convertToThrift(), out.getOutputStream());

    out.close();
    fos.close();

    FileInputStream fis = new FileInputStream(new File(PATH));
    Utils.isVSeriesChunkMetadataEqual(metaData, metaData.convertToThrift());
    Utils.isVSeriesChunkMetadataEqual(metaData,
    		ReadWriteThriftFormatUtils.read(fis, new ValueInTimeSeriesChunkMetaData()));
  }

  @Test
  public void testConvertToThrift() throws UnsupportedEncodingException {
    for (TSDataType dataType : TSDataType.values()) {
      VInTimeSeriesChunkMetaData metaData = new VInTimeSeriesChunkMetaData(dataType);
      Utils.isVSeriesChunkMetadataEqual(metaData, metaData.convertToThrift());

      metaData.setMaxError(3123);
      Utils.isVSeriesChunkMetadataEqual(metaData, metaData.convertToThrift());
      metaData.setMaxError(-11);
      Utils.isVSeriesChunkMetadataEqual(metaData, metaData.convertToThrift());

      ByteBuffer max = ByteBuffer.wrap(maxString.getBytes("UTF-8"));
      ByteBuffer min = ByteBuffer.wrap(minString.getBytes("UTF-8"));
      TSDigest digest = new TSDigest();
      metaData.setDigest(digest);
      Utils.isVSeriesChunkMetadataEqual(metaData, metaData.convertToThrift());
      digest.max = max;
      digest.min = min;
      metaData.setDigest(digest);
      Utils.isVSeriesChunkMetadataEqual(metaData, metaData.convertToThrift());
    }
  }

  @Test
  public void testConvertToTSF() throws UnsupportedEncodingException {
    for (DataType dataType : DataType.values()) {
      ValueInTimeSeriesChunkMetaData valueInTimeSeriesChunkMetaData =
          new ValueInTimeSeriesChunkMetaData(dataType);
      metaData.convertToTSF(valueInTimeSeriesChunkMetaData);
      Utils.isVSeriesChunkMetadataEqual(metaData, valueInTimeSeriesChunkMetaData);

      valueInTimeSeriesChunkMetaData.setMax_error(3123);
      metaData.convertToTSF(valueInTimeSeriesChunkMetaData);
      Utils.isVSeriesChunkMetadataEqual(metaData, valueInTimeSeriesChunkMetaData);

      valueInTimeSeriesChunkMetaData.setMax_error(-231);
      metaData.convertToTSF(valueInTimeSeriesChunkMetaData);
      Utils.isVSeriesChunkMetadataEqual(metaData, valueInTimeSeriesChunkMetaData);

      ByteBuffer max = ByteBuffer.wrap(maxString.getBytes("UTF-8"));
      ByteBuffer min = ByteBuffer.wrap(minString.getBytes("UTF-8"));
      Digest digest = new Digest();
      valueInTimeSeriesChunkMetaData.setDigest(digest);
      metaData.convertToTSF(valueInTimeSeriesChunkMetaData);
      Utils.isVSeriesChunkMetadataEqual(metaData, valueInTimeSeriesChunkMetaData);

      digest.max = max;
      digest.min = min;
      valueInTimeSeriesChunkMetaData.setDigest(digest);
      metaData.convertToTSF(valueInTimeSeriesChunkMetaData);
      Utils.isVSeriesChunkMetadataEqual(metaData, valueInTimeSeriesChunkMetaData);
    }
  }
}

package cn.edu.tsinghua.tsfile.file.metadata;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;

import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.file.metadata.utils.TestHelper;
import cn.edu.tsinghua.tsfile.file.utils.ReadWriteToBytesUtils;
import cn.edu.tsinghua.tsfile.common.utils.TsRandomAccessFileWriter;
import cn.edu.tsinghua.tsfile.file.metadata.utils.Utils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class VInTimeSeriesChunkMetaDataTest {
  private VInTimeSeriesChunkMetaData metaData;
  public static final int MAX_ERROR = 1232;
//  public static final String maxString = "3244324";
//  public static final String minString = "fddsfsfgd";
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
    VInTimeSeriesChunkMetaData metaData = TestHelper.createSimpleV2InTSF(TSDataType.TEXT, new TsDigest());
    
    File file = new File(PATH);
    if (file.exists())
      file.delete();
    FileOutputStream fos = new FileOutputStream(file);
    TsRandomAccessFileWriter out = new TsRandomAccessFileWriter(file, "rw");
    ReadWriteToBytesUtils.write(metaData, out.getOutputStream());

    out.close();
    fos.close();

    FileInputStream fis = new FileInputStream(new File(PATH));
    VInTimeSeriesChunkMetaData metaData2 = ReadWriteToBytesUtils.readVInTimeSeriesChunkMetaData(fis);
    fis.close();
    Utils.isVSeriesChunkMetadataEqual(metaData, metaData2);
  }
}

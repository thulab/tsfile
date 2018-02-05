package cn.edu.tsinghua.tsfile.file.metadata;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.file.utils.ReadWriteToBytesUtils;
import cn.edu.tsinghua.tsfile.common.utils.TsRandomAccessFileWriter;
import cn.edu.tsinghua.tsfile.file.metadata.utils.TestHelper;
import cn.edu.tsinghua.tsfile.file.metadata.utils.Utils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import cn.edu.tsinghua.tsfile.file.metadata.enums.TSFreqType;

public class TInTimeSeriesChunkMetaDataTest {
  private TInTimeSeriesChunkMetaData metaData;
  public static List<Integer> frequencies1;
  public static List<Integer> frequencies2;
  public static final long startTime = 523372036854775806L;
  public static final long endTime = 523372036854775806L;
  final String PATH = "target/outputT.ksn";

  @Before
  public void setUp() throws Exception {
    metaData = new TInTimeSeriesChunkMetaData();
    frequencies1 = new ArrayList<Integer>();

    frequencies2 = new ArrayList<Integer>();
    frequencies2.add(132);
    frequencies2.add(432);
    frequencies2.add(35435);
  }

  @After
  public void tearDown() throws Exception {
    File file = new File(PATH);
    if (file.exists())
      file.delete();
  }


  @Test
  public void testWriteIntoFile() throws IOException {
    TInTimeSeriesChunkMetaData metaData = TestHelper.createT2inTSF(TSDataType.TEXT,
            TSFreqType.IRREGULAR_FREQ, frequencies2, startTime, endTime);
    File file = new File(PATH);
    if (file.exists())
      file.delete();
    FileOutputStream fos = new FileOutputStream(file);
    TsRandomAccessFileWriter out = new TsRandomAccessFileWriter(file, "rw");
    ReadWriteToBytesUtils.write(metaData, out.getOutputStream());

    out.close();
    fos.close();

    FileInputStream fis = new FileInputStream(new File(PATH));
    TInTimeSeriesChunkMetaData metaData2 = ReadWriteToBytesUtils.readTInTimeSeriesChunkMetaData(fis);
    fis.close();
    Utils.isTSeriesChunkMetadataEqual(metaData, metaData2);
  }
}

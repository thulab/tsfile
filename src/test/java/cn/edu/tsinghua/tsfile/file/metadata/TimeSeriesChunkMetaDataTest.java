package cn.edu.tsinghua.tsfile.file.metadata;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import cn.edu.tsinghua.tsfile.file.metadata.utils.TestHelper;
import cn.edu.tsinghua.tsfile.file.metadata.utils.Utils;
import cn.edu.tsinghua.tsfile.file.utils.ReadWriteToBytesUtils;
import cn.edu.tsinghua.tsfile.common.utils.TsRandomAccessFileWriter;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

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
    TsRandomAccessFileWriter out = new TsRandomAccessFileWriter(file, "rw");
      ReadWriteToBytesUtils.write(metaData, out.getOutputStream());

    out.close();
    fos.close();

    FileInputStream fis = new FileInputStream(new File(PATH));
    TimeSeriesChunkMetaData metaData2 = ReadWriteToBytesUtils.readTimeSeriesChunkMetaData(fis);
    fis.close();
    Utils.isTimeSeriesChunkMetaDataEqual(metaData, metaData2);
  }
}

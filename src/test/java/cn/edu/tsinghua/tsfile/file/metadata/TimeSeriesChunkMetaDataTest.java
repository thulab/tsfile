package cn.edu.tsinghua.tsfile.file.metadata;

import cn.edu.tsinghua.tsfile.common.utils.ReadWriteIOUtils;
import cn.edu.tsinghua.tsfile.common.utils.TsRandomAccessFileWriter;
import cn.edu.tsinghua.tsfile.file.metadata.enums.CompressionType;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSEncoding;
import cn.edu.tsinghua.tsfile.file.metadata.utils.TestHelper;
import cn.edu.tsinghua.tsfile.file.metadata.utils.Utils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;

public class TimeSeriesChunkMetaDataTest {

  public static final String MEASUREMENT_UID = "sensor231";
  public static final long FILE_OFFSET = 2313424242L;
  public static final long DIGEST_OFFSET = 42354334L;
  public static final CompressionType COMPRESSION_TYPE = CompressionType.SNAPPY;
  public static final long NUM_OF_POINTS = 123456L;
  public static final long TOTAL_BYTE_SIZE = 34243453L;
  public static final long START_TIME = 523372036854775806L;
  public static final long END_TIME = 523372036854775806L;
  public static final TSDataType DATA_TYPE = TSDataType.INT64;
  public static final TSEncoding ENCODING_TYPE = TSEncoding.GORILLA;
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
    TimeSeriesChunkMetaData metaData = TestHelper.createSimpleTimeSeriesChunkMetaData();
    File file = new File(PATH);
    if (file.exists())
      file.delete();
    FileOutputStream fos = new FileOutputStream(file);
    TsRandomAccessFileWriter out = new TsRandomAccessFileWriter(file, "rw");
    ReadWriteIOUtils.write(metaData, out.getOutputStream());
    out.close();
    fos.close();

    FileInputStream fis = new FileInputStream(new File(PATH));
    Utils.isTimeSeriesChunkMetadataEqual(metaData, ReadWriteIOUtils.readTimeSeriesChunkMetaData(fis));
  }
}

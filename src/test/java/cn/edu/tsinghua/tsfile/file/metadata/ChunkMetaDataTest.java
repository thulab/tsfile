package cn.edu.tsinghua.tsfile.file.metadata;

import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.file.metadata.utils.TestHelper;
import cn.edu.tsinghua.tsfile.file.metadata.utils.Utils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;

public class ChunkMetaDataTest {

  public static final String MEASUREMENT_UID = "sensor231";
  public static final long FILE_OFFSET = 2313424242L;
  public static final long NUM_OF_POINTS = 123456L;
  public static final long START_TIME = 523372036854775806L;
  public static final long END_TIME = 523372036854775806L;
  public static final TSDataType DATA_TYPE = TSDataType.INT64;
  final String PATH = "target/outputTimeSeriesChunk.tsfile";

  @Before
  public void setUp() {}

  @After
  public void tearDown() {
    File file = new File(PATH);
    if (file.exists())
      file.delete();
  }

  @Test
  public void testWriteIntoFile() throws IOException {
    ChunkMetaData metaData = TestHelper.createSimpleTimeSeriesChunkMetaData();
    File file = new File(PATH);
    if (file.exists())
      file.delete();
    FileOutputStream fos = new FileOutputStream(file);
    metaData.serializeTo(fos);
    fos.close();

    FileInputStream fis = new FileInputStream(new File(PATH));
    Utils.isTimeSeriesChunkMetadataEqual(metaData, ChunkMetaData.deserializeFrom(fis));
  }
}

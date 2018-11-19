package cn.edu.tsinghua.tsfile.file.metadata;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;

import cn.edu.tsinghua.tsfile.common.utils.ReadWriteIOUtils;
import cn.edu.tsinghua.tsfile.file.metadata.utils.TestHelper;
import cn.edu.tsinghua.tsfile.file.metadata.utils.Utils;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class ChunkGroupMetaDataTest {

  public static final String DELTA_OBJECT_UID = "delta-3312";
  final String PATH = "target/outputRowGroup.tsfile";

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
    ChunkGroupMetaData metaData = TestHelper.createSimpleRowGroupMetaData();
    File file = new File(PATH);
    if (file.exists())
      file.delete();
    FileOutputStream fos = new FileOutputStream(file);
    metaData.serializeTo(fos);
    fos.close();

    FileInputStream fis = new FileInputStream(new File(PATH));
    Utils.isRowGroupMetaDataEqual(metaData, ChunkGroupMetaData.deserializeFrom(fis));
  }
}

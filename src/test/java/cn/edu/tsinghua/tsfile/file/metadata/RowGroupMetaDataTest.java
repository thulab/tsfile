package cn.edu.tsinghua.tsfile.file.metadata;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;

import cn.edu.tsinghua.tsfile.file.metadata.utils.TestHelper;
import cn.edu.tsinghua.tsfile.file.metadata.utils.Utils;

import cn.edu.tsinghua.tsfile.common.utils.TsRandomAccessFileWriter;
import cn.edu.tsinghua.tsfile.file.utils.ReadWriteToBytesUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class RowGroupMetaDataTest {

  public static final String DELTA_OBJECT_UID = "delta-3312";
  public static final long MAX_NUM_ROWS = 34432432432L;
  public static final long TOTAL_BYTE_SIZE = 434235463L;
  public static final String FILE_PATH = "/home/user/dev";
  public static final String DELTA_OBJECT_TYPE = "device_type_good";
  final String PATH = "target/outputRowGroup.ksn";

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
    RowGroupMetaData metaData = TestHelper.createSimpleRowGroupMetaDataInTSF();
    File file = new File(PATH);
    if (file.exists())
      file.delete();
    FileOutputStream fos = new FileOutputStream(file);
    TsRandomAccessFileWriter out = new TsRandomAccessFileWriter(file, "rw");
    ReadWriteToBytesUtils.write(metaData, out.getOutputStream());

    out.close();
    fos.close();

    FileInputStream fis = new FileInputStream(new File(PATH));
    RowGroupMetaData metaData2 = ReadWriteToBytesUtils.readRowGroupMetaData(fis);

    Utils.isRowGroupMetaDataEqual(metaData, metaData2);
  }
}

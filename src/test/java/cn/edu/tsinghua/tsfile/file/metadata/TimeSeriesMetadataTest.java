package cn.edu.tsinghua.tsfile.file.metadata;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;

import cn.edu.tsinghua.tsfile.common.utils.TsRandomAccessFileWriter;
import cn.edu.tsinghua.tsfile.file.metadata.utils.TestHelper;
import cn.edu.tsinghua.tsfile.file.utils.ReadWriteToBytesUtils;
import org.junit.Test;

import cn.edu.tsinghua.tsfile.file.metadata.utils.Utils;

public class TimeSeriesMetadataTest {
  public static final String measurementUID = "sensor01";
  public static final int typeLength = 1024;
  final String PATH = "target/outputTimeSeries.ksn";

  @Test
  public void testWriteIntoFile() throws IOException {
    TimeSeriesMetadata metaData = TestHelper.createSimpleTimeSeriesInTSF();
    File file = new File(PATH);
    if (file.exists())
      file.delete();
    FileOutputStream fos = new FileOutputStream(file);
    TsRandomAccessFileWriter out = new TsRandomAccessFileWriter(file, "rw");
    ReadWriteToBytesUtils.write(metaData, out.getOutputStream());

    out.close();
    fos.close();

    FileInputStream fis = new FileInputStream(new File(PATH));
    TimeSeriesMetadata metaData2 = ReadWriteToBytesUtils.readTimeSeriesMetadata(fis);
    fis.close();
    Utils.isTimeSeriesEqual(metaData, metaData2);
  }
}

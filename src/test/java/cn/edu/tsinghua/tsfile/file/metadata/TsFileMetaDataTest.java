package cn.edu.tsinghua.tsfile.file.metadata;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import cn.edu.tsinghua.tsfile.file.metadata.utils.Utils;
import cn.edu.tsinghua.tsfile.file.utils.ReadWriteToBytesUtils;
import cn.edu.tsinghua.tsfile.file.metadata.utils.TestHelper;
import cn.edu.tsinghua.tsfile.common.utils.TsRandomAccessFileWriter;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TsFileMetaDataTest {
  final String PATH = "target/output1.ksn";
  final int VERSION = 123;

  public static Map<String, String> properties = new HashMap<>();
  public static Map<String, TsDeltaObject> tsDeltaObjectMap = new HashMap<>();
  static {
      properties.put("s1", "sensor1");
      properties.put("s2", "sensor2");
      properties.put("s3", "sensor3");
  }
  
  static {
	  tsDeltaObjectMap.put("d1", new TsDeltaObject(123, 456, 789, 901));
	  tsDeltaObjectMap.put("d2", new TsDeltaObject(123, 456, 789, 901));
	  tsDeltaObjectMap.put("d3", new TsDeltaObject(123, 456, 789, 901));
  }

  @Before
  public void setUp() throws Exception {
  }

  @After
  public void tearDown() throws Exception {
    File file = new File(PATH);
    if (file.exists())
      file.delete();
  }

  @Test
  public void testWriteFileMetaData() throws IOException {
    TsFileMetaData tsfMetaData = new TsFileMetaData(tsDeltaObjectMap, null, VERSION);
    tsfMetaData.addTimeSeriesMetaData(TestHelper.createSimpleTimeSeriesInTSF());
    tsfMetaData.addTimeSeriesMetaData(TestHelper.createSimpleTimeSeriesInTSF());
    tsfMetaData.setCreatedBy("tsf");
    List<String> jsonMetaData = new ArrayList<String>();
    jsonMetaData.add("fsdfsfsd");
    jsonMetaData.add("424fd");
    tsfMetaData.setJsonMetaData(jsonMetaData);

    tsfMetaData.setProps(properties);
    tsfMetaData.addProp("key1", "value1");

    File file = new File(PATH);
    if (file.exists())
      file.delete();
    TsRandomAccessFileWriter out = new TsRandomAccessFileWriter(file, "rw");
    BufferedOutputStream bufferedOutputStream = new BufferedOutputStream(out.getOutputStream());
    ReadWriteToBytesUtils.write(tsfMetaData, bufferedOutputStream);

    bufferedOutputStream.close();
    out.close();

    FileInputStream fis = new FileInputStream(new File(PATH));
    TsFileMetaData tsfMetaData2 = ReadWriteToBytesUtils.readTsFileMetaData(new BufferedInputStream(fis));
    Utils.isFileMetaDataEqual(tsfMetaData, tsfMetaData2);
  }

}

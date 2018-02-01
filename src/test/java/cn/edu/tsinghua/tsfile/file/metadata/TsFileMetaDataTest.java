package cn.edu.tsinghua.tsfile.file.metadata;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import cn.edu.tsinghua.tsfile.file.metadata.converter.TsFileMetaDataConverter;
import cn.edu.tsinghua.tsfile.file.metadata.utils.Utils;
import cn.edu.tsinghua.tsfile.file.utils.ReadWriteToBytesUtils;
import cn.edu.tsinghua.tsfile.format.TimeSeries;
import cn.edu.tsinghua.tsfile.file.metadata.utils.TestHelper;
import cn.edu.tsinghua.tsfile.file.utils.ReadWriteThriftFormatUtils;
import cn.edu.tsinghua.tsfile.common.utils.TsRandomAccessFileWriter;
import cn.edu.tsinghua.tsfile.format.DeltaObject;
import cn.edu.tsinghua.tsfile.format.FileMetaData;
import com.sun.org.apache.regexp.internal.RE;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TsFileMetaDataTest {
  private TsFileMetaDataConverter converter = new TsFileMetaDataConverter();
  final String PATH = "target/output1.ksn";
  final int VERSION = 123;
  final String BYTE_FILE_PATH = "src/test/resources/bytes.txt";

  public static Map<String, String> properties = new HashMap<>();
  public static Map<String, TsDeltaObject> tsDeltaObjectMap = new HashMap<>();
  public static Map<String, DeltaObject> deltaObjectMap = new HashMap<>();
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
  
  static {
	  deltaObjectMap.put("d1", new DeltaObject(123, 456, 789, 901));
	  deltaObjectMap.put("d2", new DeltaObject(123, 456, 789, 901));
	  deltaObjectMap.put("d3", new DeltaObject(123, 456, 789, 901));
  }
  
  @Before
  public void setUp() throws Exception {
    converter = new TsFileMetaDataConverter();
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
    ReadWriteThriftFormatUtils.writeFileMetaData(converter.toThriftFileMetadata(tsfMetaData),
        out.getOutputStream());

    out.close();

    FileInputStream fis = new FileInputStream(new File(PATH));

    FileMetaData fileMetaData2 = ReadWriteThriftFormatUtils.readFileMetaData(fis);
    Utils.isFileMetaDataEqual(tsfMetaData, fileMetaData2);
  }

  @Test
  public void testWriteFileMetaDataByBytes() throws IOException {
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

  @Test
  public void thriftAndBytesSpeedComparison() throws IOException {
    int looptime = 1;
    long starttime, endtime, thrifttime, bytestime;
    thrifttime = bytestime = 0;
    TsFileMetaData metaDataFromThrift = new TsFileMetaData();
    TsFileMetaData metaDataFromBytes = new TsFileMetaData();

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

    File file = new File(BYTE_FILE_PATH);
    if (file.exists())
      file.delete();
    TsRandomAccessFileWriter out = new TsRandomAccessFileWriter(file, "rw");

    starttime = System.currentTimeMillis();
    for(int i = 0;i < looptime;i++) {
      ReadWriteThriftFormatUtils.writeFileMetaData(converter.toThriftFileMetadata(tsfMetaData),
              out.getOutputStream());
    }
    endtime = System.currentTimeMillis();
    thrifttime += endtime - starttime;
    out.close();

    FileInputStream fis = new FileInputStream(new File(BYTE_FILE_PATH));
    starttime = System.currentTimeMillis();
    for(int i = 0;i < looptime;i++) {
      metaDataFromThrift.convertToTSF(ReadWriteThriftFormatUtils.readFileMetaData(fis));
    }
    endtime = System.currentTimeMillis();
    thrifttime += endtime - starttime;
    fis.close();

    file = new File(BYTE_FILE_PATH);
    if (file.exists())
      file.delete();
    out = new TsRandomAccessFileWriter(file, "rw");
    starttime = System.currentTimeMillis();
    for(int i = 0;i < looptime;i++) {
      ReadWriteToBytesUtils.write(tsfMetaData, out.getOutputStream());
    }
    endtime = System.currentTimeMillis();
    bytestime += endtime - starttime;
    out.close();

    fis = new FileInputStream(new File(BYTE_FILE_PATH));
    starttime = System.currentTimeMillis();
    for(int i = 0;i < looptime;i++) {
      metaDataFromBytes = ReadWriteToBytesUtils.readTsFileMetaData(fis);
    }
    endtime = System.currentTimeMillis();
    bytestime += endtime - starttime;

    Utils.isFileMetaDataEqual(tsfMetaData, metaDataFromThrift);
    Utils.isFileMetaDataEqual(tsfMetaData, metaDataFromBytes);
    System.out.println(thrifttime);
    System.out.println(bytestime);
  }

  @Test
  public void simpleTest() throws IOException {
    int looptime = 10000;
    long starttime, endtime;

    System.out.println("thrift:");
    starttime = System.currentTimeMillis();
    for(int i = 0;i < looptime;i++)
      testWriteFileMetaData();
    endtime = System.currentTimeMillis();
    System.out.println(endtime - starttime);

    File file = new File(PATH);
    if(file.exists()){
      System.out.println(file.length());
      file.delete();
    }

    System.out.println("bytes:");
    starttime = System.currentTimeMillis();
    for(int i = 0;i < looptime;i++)
      testWriteFileMetaDataByBytes();
    endtime = System.currentTimeMillis();
    System.out.println(endtime - starttime);

    file = new File(PATH);
    if(file.exists())
      System.out.println(file.length());
  }

  @Test
  public void testCreateFileMetaDataInThrift() throws UnsupportedEncodingException {
    TsFileMetaData tsfMetaData = new TsFileMetaData(tsDeltaObjectMap, null, VERSION);
    Utils.isFileMetaDataEqual(tsfMetaData, converter.toThriftFileMetadata(tsfMetaData));

    tsfMetaData.setCreatedBy("tsf");
    Utils.isFileMetaDataEqual(tsfMetaData, converter.toThriftFileMetadata(tsfMetaData));

    List<String> jsonMetaData = new ArrayList<String>();
    tsfMetaData.setJsonMetaData(jsonMetaData);
    Utils.isFileMetaDataEqual(tsfMetaData, converter.toThriftFileMetadata(tsfMetaData));
    jsonMetaData.add("fsdfsfsd");
    jsonMetaData.add("424fd");
    Utils.isFileMetaDataEqual(tsfMetaData, converter.toThriftFileMetadata(tsfMetaData));

    tsfMetaData.setProps(properties);
    Utils.isFileMetaDataEqual(tsfMetaData, converter.toThriftFileMetadata(tsfMetaData));
   
    tsfMetaData.setTimeSeriesList(new ArrayList<TimeSeriesMetadata>());
    Utils.isFileMetaDataEqual(tsfMetaData, converter.toThriftFileMetadata(tsfMetaData));

    tsfMetaData.addTimeSeriesMetaData(TestHelper.createSimpleTimeSeriesInTSF());
    Utils.isFileMetaDataEqual(tsfMetaData, converter.toThriftFileMetadata(tsfMetaData));
    tsfMetaData.addTimeSeriesMetaData(TestHelper.createSimpleTimeSeriesInTSF());
    Utils.isFileMetaDataEqual(tsfMetaData, converter.toThriftFileMetadata(tsfMetaData));
  }

  @Test
  public void testCreateTSFMetadata() throws UnsupportedEncodingException {
    FileMetaData fileMetaData = new FileMetaData(VERSION, deltaObjectMap, null);
    Utils.isFileMetaDataEqual(converter.toTsFileMetadata(fileMetaData), fileMetaData);

    fileMetaData.setCreated_by("tsf");
    Utils.isFileMetaDataEqual(converter.toTsFileMetadata(fileMetaData), fileMetaData);
    
    List<String> jsonMetaData = new ArrayList<String>();
    fileMetaData.setJson_metadata(jsonMetaData);
    Utils.isFileMetaDataEqual(converter.toTsFileMetadata(fileMetaData), fileMetaData);
    jsonMetaData.add("fsdfsfsd");
    jsonMetaData.add("424fd");
    fileMetaData.setJson_metadata(jsonMetaData);
    Utils.isFileMetaDataEqual(converter.toTsFileMetadata(fileMetaData), fileMetaData);

    fileMetaData.setProperties(properties);
    Utils.isFileMetaDataEqual(converter.toTsFileMetadata(fileMetaData), fileMetaData);
    
    fileMetaData.setTimeseries_list(new ArrayList<TimeSeries>());
    Utils.isFileMetaDataEqual(converter.toTsFileMetadata(fileMetaData), fileMetaData);
    fileMetaData.getTimeseries_list().add(TestHelper.createSimpleTimeSeriesInThrift());
    Utils.isFileMetaDataEqual(converter.toTsFileMetadata(fileMetaData), fileMetaData);
    fileMetaData.getTimeseries_list().add(TestHelper.createSimpleTimeSeriesInThrift());
    Utils.isFileMetaDataEqual(converter.toTsFileMetadata(fileMetaData), fileMetaData);

  }

}

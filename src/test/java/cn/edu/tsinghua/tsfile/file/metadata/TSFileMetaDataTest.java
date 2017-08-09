package cn.edu.tsinghua.tsfile.file.metadata;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import cn.edu.tsinghua.tsfile.file.metadata.converter.TSFileMetaDataConverter;
import cn.edu.tsinghua.tsfile.file.metadata.utils.Utils;
import cn.edu.tsinghua.tsfile.format.TimeSeries;
import cn.edu.tsinghua.tsfile.file.metadata.utils.TestHelper;
import cn.edu.tsinghua.tsfile.file.utils.ReadWriteThriftFormatUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import cn.edu.tsinghua.tsfile.common.utils.RandomAccessOutputStream;
import cn.edu.tsinghua.tsfile.format.FileMetaData;

public class TSFileMetaDataTest {
  private TSFileMetaDataConverter converter = new TSFileMetaDataConverter();
  final String PATH = "target/output1.ksn";

  public static Map<String, String> properties = new HashMap<>();
  static {
      properties.put("s1", "sensor1");
      properties.put("s2", "sensor2");
      properties.put("s3", "sensor3");
  }
  
  @Before
  public void setUp() throws Exception {
    converter = new TSFileMetaDataConverter();
  }

  @After
  public void tearDown() throws Exception {
    File file = new File(PATH);
    if (file.exists())
      file.delete();
  }

  @Test
  public void testWriteFileMetaData() throws IOException {
    TSFileMetaData tsfMetaData = new TSFileMetaData(null, null, 0);
    tsfMetaData.addRowGroupMetaData(TestHelper.createSimpleRowGroupMetaDataInTSF());
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
    FileOutputStream fos = new FileOutputStream(file);
    RandomAccessOutputStream out = new RandomAccessOutputStream(file, "rw");
    ReadWriteThriftFormatUtils.writeFileMetaData(converter.toThriftFileMetadata(tsfMetaData),
        out.getOutputStream());

    out.close();
    fos.close();

    FileInputStream fis = new FileInputStream(new File(PATH));

    FileMetaData fileMetaData2 =
        ReadWriteThriftFormatUtils.readFileMetaData(fis);
    Utils.isFileMetaDataEqual(tsfMetaData, fileMetaData2);
  }

  @Test
  public void testCreateFileMetaDataInThrift() throws UnsupportedEncodingException {
    TSFileMetaData tsfMetaData = new TSFileMetaData(null, null, 12);
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
    
    tsfMetaData.setRowGroups(new ArrayList<RowGroupMetaData>());
    Utils.isFileMetaDataEqual(tsfMetaData, converter.toThriftFileMetadata(tsfMetaData));
    tsfMetaData.addRowGroupMetaData(TestHelper.createSimpleRowGroupMetaDataInTSF());
    Utils.isFileMetaDataEqual(tsfMetaData, converter.toThriftFileMetadata(tsfMetaData));
    tsfMetaData.addRowGroupMetaData(TestHelper.createSimpleRowGroupMetaDataInTSF());
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
    FileMetaData fileMetaData = new FileMetaData(21, null, 0, null);
    Utils.isFileMetaDataEqual(converter.toTSFileMetadata(fileMetaData), fileMetaData);

    List<String> jsonMetaData = new ArrayList<String>();
    fileMetaData.setJson_metadata(jsonMetaData);
    Utils.isFileMetaDataEqual(converter.toTSFileMetadata(fileMetaData), fileMetaData);
    jsonMetaData.add("fsdfsfsd");
    jsonMetaData.add("424fd");
    fileMetaData.setJson_metadata(jsonMetaData);
    Utils.isFileMetaDataEqual(converter.toTSFileMetadata(fileMetaData), fileMetaData);

    fileMetaData.setTimeseries_list(new ArrayList<TimeSeries>());
    Utils.isFileMetaDataEqual(converter.toTSFileMetadata(fileMetaData), fileMetaData);

    fileMetaData.setProperties(properties);
    Utils.isFileMetaDataEqual(converter.toTSFileMetadata(fileMetaData), fileMetaData);
    
    fileMetaData.getTimeseries_list().add(TestHelper.createSimpleTimeSeriesInThrift());
    Utils.isFileMetaDataEqual(converter.toTSFileMetadata(fileMetaData), fileMetaData);
    fileMetaData.getTimeseries_list().add(TestHelper.createSimpleTimeSeriesInThrift());
    Utils.isFileMetaDataEqual(converter.toTSFileMetadata(fileMetaData), fileMetaData);

    fileMetaData.setRow_groups(new ArrayList<cn.edu.tsinghua.tsfile.format.RowGroupMetaData>());
    Utils.isFileMetaDataEqual(converter.toTSFileMetadata(fileMetaData), fileMetaData);

    cn.edu.tsinghua.tsfile.format.RowGroupMetaData rowGroupMetaData1 =
        TestHelper.createSimpleRowGroupMetaDataInThrift();
    fileMetaData.max_num_rows += rowGroupMetaData1.getMax_num_rows();
    fileMetaData.getRow_groups().add(rowGroupMetaData1);
    Utils.isFileMetaDataEqual(converter.toTSFileMetadata(fileMetaData), fileMetaData);

    cn.edu.tsinghua.tsfile.format.RowGroupMetaData rowGroupMetaData2 =
        TestHelper.createSimpleRowGroupMetaDataInThrift();
    fileMetaData.max_num_rows += rowGroupMetaData2.getMax_num_rows();
    fileMetaData.getRow_groups().add(rowGroupMetaData2);
    Utils.isFileMetaDataEqual(converter.toTSFileMetadata(fileMetaData), fileMetaData);
  }

}

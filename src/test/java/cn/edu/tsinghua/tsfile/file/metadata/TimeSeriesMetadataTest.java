package cn.edu.tsinghua.tsfile.file.metadata;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import cn.edu.tsinghua.tsfile.common.utils.TsRandomAccessFileWriter;
import cn.edu.tsinghua.tsfile.file.metadata.utils.TestHelper;
import cn.edu.tsinghua.tsfile.file.utils.ReadWriteToBytesUtils;
import org.junit.Test;

import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSFreqType;
import cn.edu.tsinghua.tsfile.file.metadata.utils.Utils;
import cn.edu.tsinghua.tsfile.format.DataType;
import cn.edu.tsinghua.tsfile.format.FreqType;
import cn.edu.tsinghua.tsfile.format.TimeSeries;

public class TimeSeriesMetadataTest {
  public static final String measurementUID = "sensor01";
  public static final int typeLength = 1024;
  final String BYTE_FILE_PATH = "src/test/resources/bytes.txt";

  @Test
  public void testWriteIntoFileByBytes() throws IOException {
    TimeSeriesMetadata metaData = TestHelper.createSimpleTimeSeriesInTSF();
    File file = new File(BYTE_FILE_PATH);
    if (file.exists())
      file.delete();
    FileOutputStream fos = new FileOutputStream(file);
    TsRandomAccessFileWriter out = new TsRandomAccessFileWriter(file, "rw");
    ReadWriteToBytesUtils.write(metaData, out.getOutputStream());

    out.close();
    fos.close();

    FileInputStream fis = new FileInputStream(new File(BYTE_FILE_PATH));
    TimeSeriesMetadata metaData2 = ReadWriteToBytesUtils.readTimeSeriesMetadata(fis);
    fis.close();
    Utils.isTimeSeriesEqual(metaData, metaData2);
  }

  @Test
  public void testConvertToThrift() {
    for (TSDataType dataType : TSDataType.values()) {
      TimeSeriesMetadata timeSeries =
          new TimeSeriesMetadata(measurementUID, dataType);
      Utils.isTimeSeriesEqual(timeSeries, timeSeries.convertToThrift());

      for (TSFreqType freqType : TSFreqType.values()) {
        timeSeries.setFreqType(freqType);
        Utils.isTimeSeriesEqual(timeSeries, timeSeries.convertToThrift());
        timeSeries.setTypeLength(typeLength);
        Utils.isTimeSeriesEqual(timeSeries, timeSeries.convertToThrift());

        List<Integer> frequencies = new ArrayList<Integer>();
        timeSeries.setFrequencies(frequencies);
        Utils.isTimeSeriesEqual(timeSeries, timeSeries.convertToThrift());

        frequencies.add(132);
        frequencies.add(432);
        frequencies.add(35435);
        timeSeries.setFrequencies(frequencies);
        Utils.isTimeSeriesEqual(timeSeries, timeSeries.convertToThrift());

        timeSeries.setFrequencies(null);
        Utils.isTimeSeriesEqual(timeSeries, timeSeries.convertToThrift());
      }
    }
  }

  @Test
  public void testConvertToTSF() {
    for (DataType dataType : DataType.values()) {
      TimeSeries timeSeries = new TimeSeries(measurementUID, dataType, "");
      TimeSeriesMetadata tsTimeSeries = new TimeSeriesMetadata();
      tsTimeSeries.convertToTSF(timeSeries);
      Utils.isTimeSeriesEqual(tsTimeSeries, timeSeries);

      for (FreqType freqType : FreqType.values()) {
        timeSeries.setFreq_type(freqType);
        tsTimeSeries.convertToTSF(timeSeries);
        Utils.isTimeSeriesEqual(tsTimeSeries, timeSeries);

        timeSeries.setType_length(typeLength);
        tsTimeSeries.convertToTSF(timeSeries);
        Utils.isTimeSeriesEqual(tsTimeSeries, timeSeries);

        List<Integer> frequencies = new ArrayList<Integer>();
        timeSeries.setFrequencies(frequencies);
        tsTimeSeries.convertToTSF(timeSeries);
        Utils.isTimeSeriesEqual(tsTimeSeries, timeSeries);

        frequencies.add(132);
        frequencies.add(432);
        frequencies.add(35435);
        timeSeries.setFrequencies(frequencies);
        tsTimeSeries.convertToTSF(timeSeries);
        Utils.isTimeSeriesEqual(tsTimeSeries, timeSeries);

        timeSeries.setFrequencies(null);
        tsTimeSeries.convertToTSF(timeSeries);
        Utils.isTimeSeriesEqual(tsTimeSeries, timeSeries);
      }
    }
  }
}

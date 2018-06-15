package cn.edu.tsinghua.ts2file.timesegment.demo;

import cn.edu.tsinghua.ts2file.timesegment.write.Ts2FileWriter;
import cn.edu.tsinghua.ts2file.timesegment.write.record.Ts2Record;
import cn.edu.tsinghua.ts2file.timesegment.write.record.SegmentDataPoint;
import cn.edu.tsinghua.ts2file.timesegment.write.record.datapoint.FloatDataPoint;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSEncoding;
import cn.edu.tsinghua.tsfile.timeseries.write.desc.MeasurementDescriptor;
import java.io.File;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by qmm on 18/3/18.
 */
public class Ts2FileWrite {

  public static void main(String[] args) throws Exception {
    long start = System.currentTimeMillis();
    Ts2FileWriter ts2FileWriter = new Ts2FileWriter(new File("test.tw"));
    Map<String, String> props = new HashMap<>();
    props.put("compressor", "SNAPPY");
    ts2FileWriter.addMeasurement(
        new MeasurementDescriptor("sum(s1)", TSDataType.FLOAT, TSEncoding.TS_2DIFF, props));
    for (int i = 0; i < 100000000; i++) {
      Ts2Record record = new Ts2Record(i, i + 1, "root.laptop.d1");
      SegmentDataPoint point = new FloatDataPoint("sum(s1)", i + 0.1f);
      record.addTuple(point);
      ts2FileWriter.write(record);
    }
    ts2FileWriter.close();
    long end = System.currentTimeMillis();
    System.out.println(end - start);
  }
}

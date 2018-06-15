package cn.edu.tsinghua.ts2file.timesegment.demo;

import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSEncoding;
import cn.edu.tsinghua.tsfile.timeseries.write.TsFileWriter;
import cn.edu.tsinghua.tsfile.timeseries.write.desc.MeasurementDescriptor;
import cn.edu.tsinghua.tsfile.timeseries.write.exception.WriteProcessException;
import cn.edu.tsinghua.tsfile.timeseries.write.record.DataPoint;
import cn.edu.tsinghua.tsfile.timeseries.write.record.TSRecord;
import cn.edu.tsinghua.tsfile.timeseries.write.record.datapoint.FloatDataPoint;
import java.io.File;
import java.io.IOException;

public class WriteTsFileDemo {

	public static void main(String[] args) throws WriteProcessException, IOException {
		TsFileWriter tsFileWriter=new TsFileWriter(new File("test.ts"));
		tsFileWriter.addMeasurement(new MeasurementDescriptor("cpu_utility", TSDataType.FLOAT, TSEncoding.TS_2DIFF));
		tsFileWriter.addMeasurement(new MeasurementDescriptor("memory_utility", TSDataType.FLOAT, TSEncoding.TS_2DIFF));		

		for (int i = 0; i < 10000002; i++) {
			TSRecord tsRecord=new TSRecord(i, "user1.thinkpad.T200");
			DataPoint dPoint1=new FloatDataPoint("cpu_utility", i + 0.1f);
			tsRecord.addTuple(dPoint1);
			tsFileWriter.write(tsRecord);
		}


		tsFileWriter.close();
	}

}

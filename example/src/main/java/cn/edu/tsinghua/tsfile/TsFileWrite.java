/**
 * There are two ways to construct a TsFile instance,they generate the same TsFile file.
 * The class use the second interface: 
 *     public void addMeasurement(MeasurementSchema measurementDescriptor) throws WriteProcessException
 */
package cn.edu.tsinghua.tsfile;

import java.io.File;

import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSEncoding;
import cn.edu.tsinghua.tsfile.timeseries.write.TsFileWriter;
import cn.edu.tsinghua.tsfile.timeseries.write.desc.MeasurementSchema;
import cn.edu.tsinghua.tsfile.timeseries.write.record.datapoint.DataPoint;
import cn.edu.tsinghua.tsfile.timeseries.write.record.TSRecord;
import cn.edu.tsinghua.tsfile.timeseries.write.record.datapoint.*;

/**
 * An example of writing data to TsFile
 */
public class TsFileWrite {

	public static void main(String args[]) {
		try {
			String path = "test.tsfile";
			File f = new File(path);
			if(f.exists()) {
				f.delete();
			}
			TsFileWriter tsFileWriter = new TsFileWriter(f);

			TSEncoding encoding = TSEncoding.RLE;
			TSDataType dataType = TSDataType.FLOAT;
			int sensorNum = 10;
			int deviceNum = 1;
			int ptNum = 100000000;
			String SENSOR_PREFIX = "s";
			String DEVICE_PREFIX = "d";

			for (int i = 0; i < sensorNum; i++) {
				MeasurementSchema descriptor = new MeasurementSchema(SENSOR_PREFIX + i, dataType, encoding);
				tsFileWriter.addMeasurement(descriptor);
			}

			for(int i = 0; i < ptNum; i ++) {
				Object value = (float) i;
				for(int j = 0; j < deviceNum; j ++) {
					TSRecord record = new TSRecord(i + 1, DEVICE_PREFIX + j);
					for (int k = 0; k < sensorNum; k++) {
						DataPoint point = new FloatDataPoint(SENSOR_PREFIX + k, (float) value);
						record.addTuple(point);
					}
					tsFileWriter.write(record);
				}
				if ((i + 1) % (ptNum / 100) == 0) {
					System.out.println(String.format("Progress: %d%%", (i + 1)*100 / ptNum));
				}
			}

			// add measurements into file schema
//			tsFileWriter.addMeasurement(new MeasurementSchema("sensor_1", TSDataType.FLOAT, TSEncoding.RLE));
//			tsFileWriter.addMeasurement(new MeasurementSchema("sensor_2", TSDataType.INT32, TSEncoding.TS_2DIFF));
//			tsFileWriter.addMeasurement(new MeasurementSchema("sensor_3", TSDataType.INT32, TSEncoding.TS_2DIFF));
//
//			// construct TSRecord
//			TSRecord tsRecord = new TSRecord(1, "device_1");
//			DataPoint dPoint1 = new FloatDataPoint("sensor_1", 1.2f);
//			DataPoint dPoint2 = new IntDataPoint("sensor_2", 20);
//			DataPoint dPoint3;
//			tsRecord.addTuple(dPoint1);
//			tsRecord.addTuple(dPoint2);
//
//			// write a TSRecord to TsFile
//			tsFileWriter.write(tsRecord);
//
//
//			tsRecord = new TSRecord(2, "device_1");
//			dPoint2 = new IntDataPoint("sensor_2", 20);
//			dPoint3 = new IntDataPoint("sensor_3", 50);
//			tsRecord.addTuple(dPoint2);
//			tsRecord.addTuple(dPoint3);
//			tsFileWriter.write(tsRecord);
//
//			tsRecord = new TSRecord(3, "device_1");
//			dPoint1 = new FloatDataPoint("sensor_1", 1.4f);
//			dPoint2 = new IntDataPoint("sensor_2", 21);
//			tsRecord.addTuple(dPoint1);
//			tsRecord.addTuple(dPoint2);
//			tsFileWriter.write(tsRecord);
//
//			tsRecord = new TSRecord(4, "device_1");
//			dPoint1 = new FloatDataPoint("sensor_1", 1.2f);
//			dPoint2 = new IntDataPoint("sensor_2", 20);
//			dPoint3 = new IntDataPoint("sensor_3", 51);
//			tsRecord.addTuple(dPoint1);
//			tsRecord.addTuple(dPoint2);
//			tsRecord.addTuple(dPoint3);
//			tsFileWriter.write(tsRecord);
//
//			tsRecord = new TSRecord(6, "device_1");
//			dPoint1 = new FloatDataPoint("sensor_1", 7.2f);
//			dPoint2 = new IntDataPoint("sensor_2", 10);
//			dPoint3 = new IntDataPoint("sensor_3", 11);
//			tsRecord.addTuple(dPoint1);
//			tsRecord.addTuple(dPoint2);
//			tsRecord.addTuple(dPoint3);
//			tsFileWriter.write(tsRecord);
//
//			tsRecord = new TSRecord(7, "device_1");
//			dPoint1 = new FloatDataPoint("sensor_1", 6.2f);
//			dPoint2 = new IntDataPoint("sensor_2", 20);
//			dPoint3 = new IntDataPoint("sensor_3", 21);
//			tsRecord.addTuple(dPoint1);
//			tsRecord.addTuple(dPoint2);
//			tsRecord.addTuple(dPoint3);
//			tsFileWriter.write(tsRecord);
//
//			tsRecord = new TSRecord(8, "device_1");
//			dPoint1 = new FloatDataPoint("sensor_1", 9.2f);
//			dPoint2 = new IntDataPoint("sensor_2", 30);
//			dPoint3 = new IntDataPoint("sensor_3", 31);
//			tsRecord.addTuple(dPoint1);
//			tsRecord.addTuple(dPoint2);
//			tsRecord.addTuple(dPoint3);
//			tsFileWriter.write(tsRecord);

			// close TsFile
			tsFileWriter.close();
		} catch (Throwable e) {
			e.printStackTrace();
			System.out.println(e.getMessage());
		}
	}

}
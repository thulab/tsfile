package cn.edu.tsinghua.tsfile.timeseries.utils;

import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSEncoding;
import cn.edu.tsinghua.tsfile.timeseries.write.TsFileWriter;
import cn.edu.tsinghua.tsfile.timeseries.write.desc.MeasurementDescriptor;
import cn.edu.tsinghua.tsfile.timeseries.write.exception.WriteProcessException;
import cn.edu.tsinghua.tsfile.timeseries.write.record.TSRecord;
import cn.edu.tsinghua.tsfile.timeseries.write.record.datapoint.DoubleDataPoint;

import java.io.File;
import java.io.IOException;

public class TsFileGenerator {

    private String SENSOR_PREFIX = "s";
    private String DEVICE_PREFIX = "d";

    private String filePath = "gen.ts_plain";
    private int sensorNum = 10;
    private int deviceNum = 10;
    private int ptNum = 1000000;

    private TSEncoding encoding = TSEncoding.PLAIN;

    private TsFileWriter writer;

    private void initWriter() throws WriteProcessException, IOException {
        writer = new TsFileWriter(new File(filePath));
        for (int i = 0; i < sensorNum; i++) {
            MeasurementDescriptor descriptor = new MeasurementDescriptor(SENSOR_PREFIX + i, TSDataType.DOUBLE, encoding);
            writer.addMeasurement(descriptor);
        }
    }

    private void gen() throws IOException, WriteProcessException {
        initWriter();
        for(int i = 0; i < ptNum; i ++) {
            for(int j = 0; j < deviceNum; j ++) {
                TSRecord record = new TSRecord(i + 1, DEVICE_PREFIX + j);
                for (int k = 0; k < sensorNum; k++) {
                    DoubleDataPoint point = new DoubleDataPoint(SENSOR_PREFIX + k, i * 1.0);
                    record.addTuple(point);
                }
                writer.write(record);
            }
            if ((i + 1) % (ptNum / 100) == 0) {
                System.out.println(String.format("Progress: %d%%", (i + 1)*100 / ptNum));
            }
        }
        writer.close();
        writer = null;
    }

    public static void main(String[] args) throws IOException, WriteProcessException {
        TsFileGenerator generator = new TsFileGenerator();
        generator.gen();
    }
}
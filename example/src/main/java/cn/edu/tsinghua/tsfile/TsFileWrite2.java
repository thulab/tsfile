package cn.edu.tsinghua.tsfile;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;

import cn.edu.tsinghua.tsfile.timeseries.write.TsFileWriter;
import cn.edu.tsinghua.tsfile.timeseries.write.schema.FileSchema;
import org.json.JSONObject;

import cn.edu.tsinghua.tsfile.timeseries.write.exception.WriteProcessException;
import cn.edu.tsinghua.tsfile.timeseries.write.record.datapoint.DataPoint;
import cn.edu.tsinghua.tsfile.timeseries.write.record.TSRecord;
import cn.edu.tsinghua.tsfile.timeseries.write.record.datapoint.FloatDataPoint;
import cn.edu.tsinghua.tsfile.timeseries.write.record.datapoint.IntDataPoint;

public class TsFileWrite2 {

    public static void main(String args[]) throws IOException, WriteProcessException {
        String path = "test.tsfile";
        String s = "{\n" +
                "    \"schema\": [\n" +
                "        {\n" +
                "            \"measurement_id\": \"sensor_1\",\n" +
                "            \"data_type\": \"FLOAT\",\n" +
                "            \"encoding\": \"RLE\"\n" +
                "        },\n" +
                "        {\n" +
                "            \"measurement_id\": \"sensor_2\",\n" +
                "            \"data_type\": \"INT32\",\n" +
                "            \"encoding\": \"TS_2DIFF\"\n" +
                "        },\n" +
                "        {\n" +
                "            \"measurement_id\": \"sensor_3\",\n" +
                "            \"data_type\": \"INT32\",\n" +
                "            \"encoding\": \"TS_2DIFF\"\n" +
                "        }\n" +
                "    ],\n" +
                "    \"row_group_size\": 134217728\n" +
                "}";
        JSONObject schemaObject = new JSONObject(s);

        FileSchema schema = new FileSchema(schemaObject);
        TsFileWriter tsFileWriter = new TsFileWriter(new File(path), schema);

        TSRecord record = new TSRecord(1, "device_1");
        record.addTuple(new FloatDataPoint("sensor_1", 1.2f));
        record.addTuple(new IntDataPoint("sensor_2", 20));
        tsFileWriter.write(record);

        record = new TSRecord(2, "device_1");
        record.addTuple(new IntDataPoint("sensor_2", 20));
        record.addTuple(new IntDataPoint("sensor_3", 50));
        tsFileWriter.write(record);

        record = new TSRecord(2, "device_1");
        record.addTuple(new FloatDataPoint("sensor_1", 1.4f));
        record.addTuple(new IntDataPoint("sensor_2", 21));
        tsFileWriter.write(record);

        record = new TSRecord(2, "device_1");
        record.addTuple(new FloatDataPoint("sensor_1", 1.2f));
        record.addTuple(new IntDataPoint("sensor_2", 20));
        record.addTuple(new IntDataPoint("sensor_3", 51));
        tsFileWriter.write(record);


        TSRecord tsRecord1 = new TSRecord(6, "device_1");
        tsRecord1.dataPointList = new ArrayList<DataPoint>() {{
            add(new FloatDataPoint("sensor_1", 7.2f));
            add(new IntDataPoint("sensor_2", 10));
            add(new IntDataPoint("sensor_3", 11));
        }};
        TSRecord tsRecord2 = new TSRecord(7, "device_1");
        tsRecord2.dataPointList = new ArrayList<DataPoint>() {{
            add(new FloatDataPoint("sensor_1", 6.2f));
            add(new IntDataPoint("sensor_2", 20));
            add(new IntDataPoint("sensor_3", 21));
        }};
        TSRecord tsRecord3 = new TSRecord(8, "device_1");
        tsRecord3.dataPointList = new ArrayList<DataPoint>() {{
            add(new FloatDataPoint("sensor_1", 9.2f));
            add(new IntDataPoint("sensor_2", 30));
            add(new IntDataPoint("sensor_3", 31));
        }};
        tsFileWriter.write(tsRecord1);
        tsFileWriter.write(tsRecord2);
        tsFileWriter.write(tsRecord3);
        tsFileWriter.close();
    }
}


package cn.edu.tsinghua.tsfile.timeseries;

import cn.edu.tsinghua.tsfile.timeseries.basis.TsFile;
import cn.edu.tsinghua.tsfile.timeseries.read.TsRandomAccessLocalFileReader;
import cn.edu.tsinghua.tsfile.timeseries.read.query.QueryDataSet;
import cn.edu.tsinghua.tsfile.timeseries.read.support.Field;
import cn.edu.tsinghua.tsfile.timeseries.read.support.Path;
import cn.edu.tsinghua.tsfile.timeseries.read.support.RowRecord;
import cn.edu.tsinghua.tsfile.timeseries.write.exception.WriteProcessException;
import cn.edu.tsinghua.tsfile.timeseries.write.record.DataPoint;
import cn.edu.tsinghua.tsfile.timeseries.write.record.TSRecord;
import cn.edu.tsinghua.tsfile.timeseries.write.record.datapoint.FloatDataPoint;
import cn.edu.tsinghua.tsfile.timeseries.write.record.datapoint.IntDataPoint;
import org.json.JSONObject;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;

import static org.junit.Assert.assertEquals;

public class multDeltaObjectWriteTest {

    private String path = "src/test/resources/multDevice.ts";
    private String schema;

    @Before
    public void init(){
        File file = new File(path);
        if(file.exists())file.delete();

        schema = "{\n" +
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
                "    \"properties\": \n" +
                "        {\n" +
                "            \"key1\": \"value1\",\n"+
                "            \"key2\": \"value2\"\n"+
                "        },\n" +
                "    \"row_group_size\": 134217728\n" +
                "}";
    }

    @After
    public void after(){
        File file = new File(path);
        if(file.exists())file.delete();
    }

    @Test
    public void test() throws IOException, WriteProcessException {
        write();
        checkContent();
    }

    private void write() throws IOException, WriteProcessException {
        JSONObject schemaObject = new JSONObject(schema);

        File file = new File(path);
        TsFile tsFile = new TsFile(file, schemaObject);

        tsFile.writeLine("device_1,11, sensor_1, 1.2, sensor_2, 20, sensor_3,");
        tsFile.writeLine("device_1,12, sensor_1, , sensor_2, 20, sensor_3, 50");
        tsFile.writeLine("device_1,13, sensor_1, 1.4, sensor_2, 21, sensor_3,");
        tsFile.writeLine("device_1,14, sensor_1, , sensor_2, 20, sensor_3, 51");

        TSRecord tsRecord1 = new TSRecord(21, "device_2");
        tsRecord1.dataPointList = new ArrayList<DataPoint>() {{
            add(new FloatDataPoint("sensor_1", 7.2f));
            add(new IntDataPoint("sensor_2", 10));
            add(new IntDataPoint("sensor_3", 11));
        }};
        TSRecord tsRecord2 = new TSRecord(22, "device_2");
        tsRecord2.dataPointList = new ArrayList<DataPoint>() {{
            add(new FloatDataPoint("sensor_1", 6.2f));
            add(new IntDataPoint("sensor_2", 20));
            add(new IntDataPoint("sensor_3", 21));
        }};
        TSRecord tsRecord3 = new TSRecord(23, "device_2");
        tsRecord3.dataPointList = new ArrayList<DataPoint>() {{
            add(new FloatDataPoint("sensor_1", 9.2f));
            add(new IntDataPoint("sensor_2", 30));
            add(new IntDataPoint("sensor_3", 31));
        }};
        tsFile.writeRecord(tsRecord1);
        tsFile.writeRecord(tsRecord2);
        tsFile.writeRecord(tsRecord3);

        tsFile.writeLine("device_3,31, sensor_1, 1.2, sensor_2, , sensor_3,");
        tsFile.writeLine("device_3,32, sensor_1, , sensor_2, 20, sensor_3,");
        tsFile.writeLine("device_3,33, sensor_1, 1.4, sensor_2, , sensor_3,");
        tsFile.writeLine("device_3,34, sensor_1, , sensor_2, , sensor_3, 100");

        tsFile.close();
    }

    private int getNotNullNum(RowRecord rowRecord){
        int count = 0;
        for(Field field : rowRecord.fields){
            if(!field.getStringValue().equals("null"))count++;
        }
        return count;
    }

    private void checkContent() throws IOException {
        TsRandomAccessLocalFileReader input = new TsRandomAccessLocalFileReader(path);
        TsFile readTsFile = new TsFile(input);
        ArrayList<Path> paths = new ArrayList<>();
        paths.add(new Path("device_1.sensor_1"));
        paths.add(new Path("device_1.sensor_2"));
        paths.add(new Path("device_1.sensor_3"));
        paths.add(new Path("device_2.sensor_1"));
        paths.add(new Path("device_2.sensor_2"));
        paths.add(new Path("device_2.sensor_3"));
        paths.add(new Path("device_3.sensor_1"));
        paths.add(new Path("device_3.sensor_2"));
        paths.add(new Path("device_3.sensor_3"));
        QueryDataSet queryDataSet = readTsFile.query(paths, null, null);
        int recordCount = 0;
        RowRecord record;
        while (queryDataSet.hasNextRecord()) {
            record = queryDataSet.getNextRecord();
            recordCount++;
            System.out.println(record);

            switch ((int)record.timestamp / 10){
                case 1:
                    assertEquals(2, getNotNullNum(record));
                    break;

                case 2:
                    assertEquals(3, getNotNullNum(record));
                    break;

                case 3:
                    assertEquals(1, getNotNullNum(record));
                    break;
            }
        }
        assertEquals(11, recordCount);
    }
}



package com.corp.delta.tsfile;

import com.corp.delta.tsfile.common.conf.TSFileConfig;
import com.corp.delta.tsfile.common.conf.TSFileDescriptor;
import com.corp.delta.tsfile.common.constant.JsonFormatConstant;
import com.corp.delta.tsfile.common.utils.RandomAccessOutputStream;
import com.corp.delta.tsfile.common.utils.TSRandomAccessFileWriter;
import com.corp.delta.tsfile.file.metadata.enums.TSDataType;
import com.corp.delta.tsfile.utils.RecordUtils;
import com.corp.delta.tsfile.write.InternalRecordWriter;
import com.corp.delta.tsfile.write.TSRecordWriteSupport;
import com.corp.delta.tsfile.write.TSRecordWriter;
import com.corp.delta.tsfile.write.WriteSupport;
import com.corp.delta.tsfile.write.exception.WriteProcessException;
import com.corp.delta.tsfile.write.io.TSFileIOWriter;
import com.corp.delta.tsfile.write.record.TSRecord;
import com.corp.delta.tsfile.write.schema.FileSchema;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Scanner;

/**
 * create a TSFile for test
 *
 * @author QJL
 */
public class CreateTSFile {

    private static final int ROW_COUNT = 10;
    private static String inputDataFile;
    private static InternalRecordWriter<TSRecord> innerWriter;

    public void createTSFile(String csvPath, String tsfilePath) throws Exception {

        inputDataFile = csvPath;
        JSONObject jsonSchema = generateTestSchema();

        File csvFile = new File(inputDataFile);
        File tsFile = new File(tsfilePath);
        if(csvFile.exists()){
            csvFile.delete();
        }
        if (tsFile.exists()) {
            tsFile.delete();
        }

        createCSVFile();

        FileSchema schema = new FileSchema(jsonSchema);
        WriteSupport<TSRecord> writeSupport = new TSRecordWriteSupport();
        TSRandomAccessFileWriter outputStream = new RandomAccessOutputStream(tsFile);
        TSFileIOWriter tsfileWriter = new TSFileIOWriter(schema, outputStream);

        innerWriter = new TSRecordWriter(TSFileDescriptor.getInstance().getConfig(), tsfileWriter, writeSupport, schema);

        // write
        writeToTSFile(schema);
    }

    private Scanner getDataFile(String path) {
        File file = new File(path);
        try {
            return new Scanner(file);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
            return null;
        }
    }

    private void writeToTSFile(FileSchema schema) throws InterruptedException, IOException, WriteProcessException {
        Scanner in = getDataFile(inputDataFile);
        assert in != null;
        while (in.hasNextLine()) {
            String str = in.nextLine();
            TSRecord record = RecordUtils.parseSimpleTupleRecord(str, schema);
            innerWriter.write(record);
        }
        innerWriter.close();
    }

    private void createCSVFile() throws IOException {
        File file = new File(inputDataFile);
        if (file.exists())
            return;
        FileWriter fw = new FileWriter(file);

        long startTime = System.currentTimeMillis();
        startTime = startTime - startTime % 1000;
        for (int i = 0; i < ROW_COUNT; i++) {
            // write d1
            String d1 = "root.car.d1," + (startTime + i) + ",s1," + (i * 10 + 2);
            fw.write(d1 + "\r\n");
            d1 = "root.car.d1," + (startTime + i) + ",s2," + (i * 20 + 2);
            fw.write(d1 + "\r\n");

            // write d2
            String d2 = "root.car.d2," + (startTime + i) + ",s1," + (i * 10 + 2);
            fw.write(d2 + "\r\n");
            d2 = "root.car.d2," + (startTime + i) + ",s2," + (i * 20 + 2);
            fw.write(d2 + "\r\n");
        }
        fw.close();

    }

    private JSONObject generateTestSchema() {
        TSFileConfig conf = TSFileDescriptor.getInstance().getConfig();
        JSONObject s1 = new JSONObject();
        s1.put(JsonFormatConstant.MEASUREMENT_UID, "s1");
        s1.put(JsonFormatConstant.DATA_TYPE, TSDataType.INT32.toString());
        s1.put(JsonFormatConstant.MEASUREMENT_ENCODING,
                conf.defaultSeriesEncoder);

        JSONObject s2 = new JSONObject();
        s2.put(JsonFormatConstant.MEASUREMENT_UID, "s2");
        s2.put(JsonFormatConstant.DATA_TYPE, TSDataType.INT32.toString());
        s2.put(JsonFormatConstant.MEASUREMENT_ENCODING,
                conf.defaultSeriesEncoder);

        JSONArray measureGroup1 = new JSONArray();
        measureGroup1.put(s1);
        measureGroup1.put(s2);

        JSONObject jsonSchema = new JSONObject();
        jsonSchema.put(JsonFormatConstant.DELTA_TYPE, "test_type");
        jsonSchema.put(JsonFormatConstant.JSON_SCHEMA, measureGroup1);
        return jsonSchema;
    }
}

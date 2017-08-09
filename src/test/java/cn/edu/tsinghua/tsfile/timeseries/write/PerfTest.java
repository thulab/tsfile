package cn.edu.tsinghua.tsfile.timeseries.write;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;
import java.util.Scanner;

import cn.edu.tsinghua.tsfile.common.conf.TSFileConfig;
import cn.edu.tsinghua.tsfile.common.constant.JsonFormatConstant;
import cn.edu.tsinghua.tsfile.common.utils.RandomAccessOutputStream;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSEncoding;
import cn.edu.tsinghua.tsfile.timeseries.utils.RecordUtils;
import cn.edu.tsinghua.tsfile.timeseries.write.exception.WriteProcessException;
import cn.edu.tsinghua.tsfile.timeseries.write.schema.FileSchema;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.tsinghua.tsfile.common.conf.TSFileDescriptor;
import cn.edu.tsinghua.tsfile.common.utils.TSRandomAccessFileWriter;
import cn.edu.tsinghua.tsfile.timeseries.utils.FileUtils;
import cn.edu.tsinghua.tsfile.timeseries.utils.FileUtils.Unit;
import cn.edu.tsinghua.tsfile.timeseries.write.io.TSFileIOWriter;
import cn.edu.tsinghua.tsfile.timeseries.write.record.TSRecord;

/**
 * This is used for performance test, no asserting. User could change {@code ROW_COUNT} for larger
 * data test.
 *
 * @author kangrong
 */
public class PerfTest {
    private static final Logger LOG = LoggerFactory.getLogger(PerfTest.class);
    public static final int ROW_COUNT = 100;
    public static InternalRecordWriter<TSRecord> innerWriter;
    static public String inputDataFile;
    static public String outputDataFile;
    static public String errorOutputDataFile;
    static public JSONObject jsonSchema;
    static public Random r = new Random();

    @Before
    public void prepare() throws IOException {
        inputDataFile = "src/test/resources/perTestInputData";
        outputDataFile = "src/test/resources/perTestOutputData.ksn";
        errorOutputDataFile = "src/test/resources/perTestErrorOutputData.ksn";
        jsonSchema = generateTestData();
        generateSampleInputDataFile();
    }

    @After
    public void after() {
        File file = new File(inputDataFile);
        if (file.exists())
            file.delete();
        file = new File(outputDataFile);
        if (file.exists())
            file.delete();
        file = new File(errorOutputDataFile);
        if (file.exists())
            file.delete();
    }
    
    static private void generateSampleInputDataFile() throws IOException {
        File file = new File(inputDataFile);
        if (file.exists())
            file.delete();
        FileWriter fw = new FileWriter(file);

        long startTime = System.currentTimeMillis();
        startTime = startTime - startTime % 1000;
        Random rm = new Random();
        for (int i = 0; i < ROW_COUNT; i++) {
            String string4 = ",s4," + (char) (97 + i % 26);
            // write d1
            String d1 = "d1," + (startTime + i) + ",s1," + (i * 10 + 1) + ",s2," + (i * 10 + 2) + string4;
            if (rm.nextInt(1000) < 100) {
                // LOG.info("write null to d1:" + (startTime + i));
                d1 = "d1," + (startTime + i) + ",s1,,s2," + (i * 10 + 2) + string4;
            }
            if (i % 5 == 0)
                d1 += ",s3," + (i * 10 + 3);
            fw.write(d1 + "\r\n");

            // write d2
            String d2 = "d2," + (startTime + i) + ",s2," + (i * 10 + 2) + ",s3," + (i * 10 + 3) + string4;
            if (rm.nextInt(1000) < 100) {
                // LOG.info("write null to d2:" + (startTime + i));
                d2 = "d2," + (startTime + i) + ",s2,,s3," + (i * 10 + 3) + string4;
            }
            if (i % 5 == 0)
                d2 += ",s1," + (i * 10 + 1);
            fw.write(d2 + "\r\n");
        }
        // write error
        String d =
                "d2,3," + (startTime + ROW_COUNT) + ",s2," + (ROW_COUNT * 10 + 2) + ",s3,"
                        + (ROW_COUNT * 10 + 3);
        fw.write(d + "\r\n");
        d = "d2," + (startTime + ROW_COUNT + 1) + ",2,s-1," + (ROW_COUNT * 10 + 2);
        fw.write(d + "\r\n");
        fw.close();
    }

    @Test
    public void writeTest() throws IOException, InterruptedException, WriteProcessException {
        write();
    }

    static public void write() throws IOException, InterruptedException, WriteProcessException {
        File file = new File(outputDataFile);
        File errorFile = new File(errorOutputDataFile);
        if (file.exists())
            file.delete();
        if (errorFile.exists())
            errorFile.delete();

        //LOG.info(jsonSchema.toString());
        FileSchema schema = new FileSchema(jsonSchema);
        WriteSupport<TSRecord> writeSupport = new TSRecordWriteSupport();
        TSRandomAccessFileWriter outputStream = new RandomAccessOutputStream(file);
        TSFileIOWriter tsfileWriter = new TSFileIOWriter(schema, outputStream);

        // TSFileDescriptor.conf.rowGroupSize = 2000;
        // TSFileDescriptor.conf.pageSize = 100;
        innerWriter = new TSRecordWriter(TSFileDescriptor.getInstance().getConfig(), tsfileWriter, writeSupport, schema);

        // write
        try {
            writeToFile(schema);
        } catch (WriteProcessException e) {
            e.printStackTrace();
        }
        LOG.info("write to file successfully!!");
    }

    static private Scanner getDataFile(String path) {
        File file = new File(path);
        try {
            Scanner in = new Scanner(file);
            return in;
        } catch (FileNotFoundException e) {
            e.printStackTrace();
            return null;
        }
    }

    static public void writeToFile(FileSchema schema) throws InterruptedException, IOException, WriteProcessException {
        Scanner in = getDataFile(inputDataFile);
        long lineCount = 0;
        long startTime = System.currentTimeMillis();
        long endTime = System.currentTimeMillis();
        assert in != null;
        while (in.hasNextLine()) {
            if (lineCount % 1000000 == 0) {
                endTime = System.currentTimeMillis();
                // logger.info("write line:{},inner space consumer:{},use
                // time:{}",lineCount,innerWriter.calculateMemSizeForEachGroup(),endTime);
                LOG.info("write line:{},use time:{}s", lineCount, (endTime - startTime) / 1000);
            }
            String str = in.nextLine();
            TSRecord record = RecordUtils.parseSimpleTupleRecord(str, schema);
            innerWriter.write(record);
            lineCount++;
        }
        endTime = System.currentTimeMillis();
        LOG.info("write line:{},use time:{}s", lineCount, (endTime - startTime) / 1000);
        innerWriter.close();
        endTime = System.currentTimeMillis();
        LOG.info("write total:{},use time:{}s", lineCount, (endTime - startTime) / 1000);
        LOG.info("src file size:{}GB", FileUtils.getLocalFileByte(inputDataFile, Unit.GB));
        LOG.info("src file size:{}MB", FileUtils.getLocalFileByte(outputDataFile, Unit.MB));
    }

    private static JSONObject generateTestData() {
        TSFileConfig conf = TSFileDescriptor.getInstance().getConfig();
        JSONObject s1 = new JSONObject();
        s1.put(JsonFormatConstant.MEASUREMENT_UID, "s1");
        s1.put(JsonFormatConstant.DATA_TYPE, TSDataType.INT64.toString());
        s1.put(JsonFormatConstant.MEASUREMENT_ENCODING,
                conf.valueEncoder);
        JSONObject s2 = new JSONObject();
        s2.put(JsonFormatConstant.MEASUREMENT_UID, "s2");
        s2.put(JsonFormatConstant.DATA_TYPE, TSDataType.INT64.toString());
        s2.put(JsonFormatConstant.MEASUREMENT_ENCODING,
                conf.valueEncoder);
        JSONObject s3 = new JSONObject();
        s3.put(JsonFormatConstant.MEASUREMENT_UID, "s3");
        s3.put(JsonFormatConstant.DATA_TYPE, TSDataType.INT64.toString());
        s3.put(JsonFormatConstant.MEASUREMENT_ENCODING,
                conf.valueEncoder);
        JSONObject s4 = new JSONObject();
        s4.put(JsonFormatConstant.MEASUREMENT_UID, "s4");
        s4.put(JsonFormatConstant.DATA_TYPE, TSDataType.TEXT.toString());
        s4.put(JsonFormatConstant.MEASUREMENT_ENCODING,
                TSEncoding.PLAIN.toString());
        JSONObject s5 = new JSONObject();
        s5.put(JsonFormatConstant.MEASUREMENT_UID, "s5");
        s5.put(JsonFormatConstant.DATA_TYPE, TSDataType.ENUMS.toString());
        s5.put(JsonFormatConstant.MEASUREMENT_ENCODING,
                TSEncoding.PLAIN.toString());
        JSONArray measureGroup1 = new JSONArray();
        measureGroup1.put(s1);
        measureGroup1.put(s2);
        measureGroup1.put(s3);
        measureGroup1.put(s4);
        measureGroup1.put(s5);

        JSONObject jsonSchema = new JSONObject();
        jsonSchema.put(JsonFormatConstant.DELTA_TYPE, "test_type");
        jsonSchema.put(JsonFormatConstant.JSON_SCHEMA, measureGroup1);
        return jsonSchema;
    }
}

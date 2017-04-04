package cn.edu.thu.tsfile.timeseries.demo;


import cn.edu.thu.tsfile.common.conf.TSFileConfig;
import cn.edu.thu.tsfile.common.conf.TSFileDescriptor;
import cn.edu.thu.tsfile.common.constant.JsonFormatConstant;
import cn.edu.thu.tsfile.common.utils.RandomAccessOutputStream;
import cn.edu.thu.tsfile.common.utils.TSRandomAccessFileWriter;
import cn.edu.thu.tsfile.timeseries.utils.FileUtils;
import cn.edu.thu.tsfile.timeseries.write.InternalRecordWriter;
import cn.edu.thu.tsfile.timeseries.write.TSRecordWriteSupport;
import cn.edu.thu.tsfile.timeseries.write.TSRecordWriter;
import cn.edu.thu.tsfile.timeseries.write.record.TSRecord;
import cn.edu.thu.tsfile.timeseries.write.schema.FileSchema;
import cn.edu.thu.tsfile.timeseries.utils.RecordUtils;
import cn.edu.thu.tsfile.timeseries.write.WriteSupport;
import cn.edu.thu.tsfile.timeseries.write.exception.WriteProcessException;
import cn.edu.thu.tsfile.timeseries.write.io.TSFileIOWriter;
import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONTokener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

/**
 * Write Demo provides a JAVA application that receives CSV file and writes to TSfile format.
 * This application requires four parameters (inputDataFilePath,outputDataFilePath, errorFile
 * and schemaFile) and one optional parameter (confFile).
 * Four parameters are needed: inputDataFilePath, outputDataFilePath, errorFile and schemaFile.
 *
 * @author kangrong
 *
 */
public class WriteDemo {
    static final Logger LOG = LoggerFactory.getLogger(WriteDemo.class);
    public static InternalRecordWriter<TSRecord> innerWriter;
    public static String inputDataFile;
    public static String outputDataFile;
    public static String errorOutputDataFile;
    public static JSONObject jsonSchema;

    private static void write() throws IOException, InterruptedException, WriteProcessException {
        File file = new File(outputDataFile);
        File errorFile = new File(errorOutputDataFile);
        if (file.exists())
            file.delete();
        if (errorFile.exists())
            errorFile.delete();

        FileSchema schema = new FileSchema(jsonSchema);
        WriteSupport<TSRecord> writeSupport = new TSRecordWriteSupport();
        TSRandomAccessFileWriter outputStream = new RandomAccessOutputStream(file);
        TSFileIOWriter tsfileWriter = new TSFileIOWriter(schema, outputStream);
        TSFileConfig conf = TSFileDescriptor.getInstance().getConfig();
        innerWriter = new TSRecordWriter(conf, tsfileWriter, writeSupport, schema);

        // write to file
        try {
            writeToFile(schema);
        } catch (WriteProcessException e) {
            e.printStackTrace();
        }
        LOG.info("write to file successfully!!");
    }

    private static void writeToFile(FileSchema schema) throws InterruptedException, IOException, WriteProcessException {
        BufferedReader br = new BufferedReader(new FileReader(inputDataFile));
        long lineCount = 0;
        long startTime = System.currentTimeMillis();
        long endTime;
        String line;
        while ((line = br.readLine()) != null) {
            if (lineCount % 1000000 == 0) {
                endTime = System.currentTimeMillis();
                LOG.info("write line:{},inner space consumer:{},use time:{}", lineCount,
                        innerWriter.updateMemSizeForAllGroup(), endTime);
                LOG.info("write line:{},use time:{}s", lineCount, (endTime - startTime) / 1000);
            }
            // String str = in.nextLine();
            TSRecord record = RecordUtils.parseSimpleTupleRecord(line, schema);
            innerWriter.write(record);
            lineCount++;
        }
        endTime = System.currentTimeMillis();
        LOG.info("write line:{},use time:{}s", lineCount, (endTime - startTime) / 1000);
        innerWriter.close();
        endTime = System.currentTimeMillis();
        LOG.info("write total:{},use time:{}s", lineCount, (endTime - startTime) / 1000);
        LOG.info("src file size:{}GB", FileUtils.getLocalFileByte(inputDataFile, FileUtils.Unit.GB));
        LOG.info("src file size:{}MB", FileUtils.getLocalFileByte(outputDataFile, FileUtils.Unit.MB));
        br.close();
    }

    public static void main(String[] args) throws JSONException, IOException, InterruptedException, WriteProcessException {
        if (args.length < 4) {
            LOG.error("\n\ninput args format error, you should run as: " +
                    "<inputDataFilePath> <outputDataFilePath> <errorFile> <schemaFile>\n");
            return;
        }
        inputDataFile = args[0];
        outputDataFile = args[1];
        errorOutputDataFile = args[2];
        System.out.println(args[3]);
        String path = args[3];
        JSONObject obj = new JSONObject(new JSONTokener(new FileReader(new File(path))));
        System.out.println(obj);
        if (!obj.has(JsonFormatConstant.JSON_SCHEMA)) {
            LOG.error("input schema format error");
            return;
        }
        jsonSchema = obj;
        System.out.println(args.length);
        write();
    }
}

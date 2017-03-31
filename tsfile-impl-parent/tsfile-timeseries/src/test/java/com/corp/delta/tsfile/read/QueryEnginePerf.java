package com.corp.delta.tsfile.read;

import com.corp.delta.tsfile.common.conf.TSFileConfig;
import com.corp.delta.tsfile.common.conf.TSFileDescriptor;
import com.corp.delta.tsfile.common.constant.JsonFormatConstant;
import com.corp.delta.tsfile.common.utils.RandomAccessOutputStream;
import com.corp.delta.tsfile.common.utils.TSRandomAccessFileWriter;
import com.corp.delta.tsfile.file.metadata.enums.TSDataType;
import com.corp.delta.tsfile.utils.FileUtils;
import com.corp.delta.tsfile.utils.FileUtils.Unit;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Scanner;

public class QueryEnginePerf {
	private static final Logger LOG = LoggerFactory.getLogger(QueryEnginePerf.class);
	public static final int ROW_COUNT = 199;
	public static InternalRecordWriter<TSRecord> innerWriter;
	static public String inputDataFile;
	static public String outputDataFile;
	static public String errorOutputDataFile;
	static public JSONObject jsonSchema;

	public static void generateFile() throws IOException, InterruptedException, WriteProcessException {
		prepare();
		write();
	}

	public static void prepare() throws IOException {
		inputDataFile = "src/test/resources/perTestInputData";
		outputDataFile = "src/test/resources/perTestOutputData.ksn";
		errorOutputDataFile = "src/test/resources/perTestErrorOutputData.ksn";
		jsonSchema = generateTestData();
		generateSampleInputDataFile();
	}

	public static void after() {
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
		file.getParentFile().mkdirs();
		FileWriter fw = new FileWriter(file);

		long startTime = 1L;
		int i;
		for (i = 0; i < 169; i++) {
			String d1 = "root.vehicle.d1," + (startTime + i) + ",s1,,s2," + (1.0 * (i + 1) * 100 + 2) + ",s3," + ((i + 1) * 100 + 3);
			fw.write(d1 + "\r\n");
		}

		for (; i < 170; i++) {
			String d1 = "root.vehicle.d1,170,s1,1000001,s2,17002.0,s3,17003";
			fw.write(d1 + "\r\n");
		}
		for (; i < 179; i++) {
			String d1 = "root.vehicle.d1," + (startTime + i) + ",s1,,s2," + (1.0 * (i + 1) * 100 + 2) + ",s3," + ((i + 1) * 100 + 3);
			fw.write(d1 + "\r\n");
		}
		for (; i < 189; i++) {
			String d1 = "root.vehicle.d1," + (startTime + i) + ",s1," + ((i + 1) * 100 + 1) + ",s2," + (1.0 * (i + 1) * 100 + 2)
					+ ",s3," + ((i + 1) * 100 + 3);
			fw.write(d1 + "\r\n");
		}
		String d1 = "root.vehicle.d1," + (startTime + i) + ",s1,1,s2," + (1.0 * (i + 1) * 100 + 2) + ",s3," + ((i + 1) * 100 + 3);
		fw.write(d1 + "\r\n");
		i++;
		d1 = "root.vehicle.d1," + (startTime + i) + ",s1,1,s2," + (1.0 * (i + 1) * 100 + 2) + ",s3," + ((i + 1) * 100 + 3);
		fw.write(d1 + "\r\n");
		i++;
		d1 = "root.vehicle.d1," + (startTime + i) + ",s1,2,s2," + (1.0 * (i + 1) * 100 + 2) + ",s3," + ((i + 1) * 100 + 3);
		fw.write(d1 + "\r\n");
		i++;
		d1 = "root.vehicle.d1," + (startTime + i) + ",s1,2,s2," + (1.0 * (i + 1) * 100 + 2) + ",s3," + ((i + 1) * 100 + 3);
		fw.write(d1 + "\r\n");
		i++;
		d1 = "root.vehicle.d1," + (startTime + i) + ",s1,3,s2," + (1.0 * (i + 1) * 100 + 2) + ",s3," + ((i + 1) * 100 + 3);
		fw.write(d1 + "\r\n");
		i++;
		d1 = "root.vehicle.d1," + (startTime + i) + ",s1,3,s2," + (1.0 * (i + 1) * 100 + 2) + ",s3," + ((i + 1) * 100 + 3);
		fw.write(d1 + "\r\n");
		i++;
		d1 = "root.vehicle.d1," + (startTime + i) + ",s1,3,s2," + (1.0 * (i + 1) * 100 + 2) + ",s3," + ((i + 1) * 100 + 3);
		fw.write(d1 + "\r\n");
		i++;
		d1 = "root.vehicle.d1," + (startTime + i) + ",s1,3,s2," + (1.0 * (i + 1) * 100 + 2) + ",s3," + ((i + 1) * 100 + 3);
		fw.write(d1 + "\r\n");
		i++;
		for (; i < 199; i++) {
			d1 = "root.vehicle.d1," + (startTime + i) + ",s1," + ((i + 1) * 100 + 1) + ",s2," + (1.0 * (i + 1) * 100 + 2) + ",s3,"
					+ ((i + 1) * 100 + 3);
			fw.write(d1 + "\r\n");
		}
		fw.close();
	}

	static public void write() throws IOException, InterruptedException, WriteProcessException {
		File file = new File(outputDataFile);
		File errorFile = new File(errorOutputDataFile);
		if (file.exists())
			file.delete();
		if (errorFile.exists())
			errorFile.delete();

		// LOG.info(jsonSchema.toString());
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

	private static JSONObject generateTestData() {
		TSFileConfig conf = TSFileDescriptor.getInstance().getConfig();
		JSONObject s1 = new JSONObject();
		s1.put(JsonFormatConstant.MEASUREMENT_UID, "s1");
		s1.put(JsonFormatConstant.DATA_TYPE, TSDataType.INT32.toString());
		s1.put(JsonFormatConstant.MEASUREMENT_ENCODING, conf.defaultSeriesEncoder);
		JSONObject s2 = new JSONObject();
		s2.put(JsonFormatConstant.MEASUREMENT_UID, "s2");
		s2.put(JsonFormatConstant.DATA_TYPE, TSDataType.FLOAT.toString());
		s2.put(JsonFormatConstant.MEASUREMENT_ENCODING, conf.defaultSeriesEncoder);
		JSONObject s3 = new JSONObject();
		s3.put(JsonFormatConstant.MEASUREMENT_UID, "s3");
		s3.put(JsonFormatConstant.DATA_TYPE, TSDataType.INT64.toString());
		s3.put(JsonFormatConstant.MEASUREMENT_ENCODING, conf.defaultSeriesEncoder);

		JSONArray measureGroup1 = new JSONArray();
		measureGroup1.put(s1);
		measureGroup1.put(s2);
		measureGroup1.put(s3);

		JSONObject jsonSchema = new JSONObject();
		jsonSchema.put(JsonFormatConstant.DELTA_TYPE, "vehicle");
		jsonSchema.put(JsonFormatConstant.JSON_SCHEMA, measureGroup1);
		// System.out.println(jsonSchema);
		return jsonSchema;
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
}

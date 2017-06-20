package cn.edu.thu.tsfile.timeseries.write;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;

import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import cn.edu.thu.tsfile.common.conf.TSFileConfig;
import cn.edu.thu.tsfile.common.conf.TSFileDescriptor;
import cn.edu.thu.tsfile.common.constant.JsonFormatConstant;
import cn.edu.thu.tsfile.common.utils.RandomAccessOutputStream;
import cn.edu.thu.tsfile.common.utils.TSRandomAccessFileWriter;
import cn.edu.thu.tsfile.file.metadata.enums.TSDataType;
import cn.edu.thu.tsfile.timeseries.read.query.DynamicOneColumnData;
import cn.edu.thu.tsfile.timeseries.utils.RecordUtils;
import cn.edu.thu.tsfile.timeseries.write.exception.WriteProcessException;
import cn.edu.thu.tsfile.timeseries.write.io.TSFileIOWriter;
import cn.edu.thu.tsfile.timeseries.write.record.TSRecord;
import cn.edu.thu.tsfile.timeseries.write.schema.FileSchema;

public class ReadPageInMemTest {

	private String filePath = "TsFileReadPageInMem";
	private File file = new File(filePath);
	private TSFileConfig conf = TSFileDescriptor.getInstance().getConfig();
	private WriteSupport<TSRecord> writeSupport = new TSRecordWriteSupport();
	private InternalRecordWriter<TSRecord> innerWriter;
	private FileSchema fileSchema = null;

	@Before
	public void setUp() throws Exception {
		file.delete();
		conf.pageSize = 200;
		conf.rowGroupSize = 100000;
		conf.pageCheckSizeThreshold = 1;
		conf.defaultMaxStringLength = 2;
		conf.cachePageData = true;
		TSRandomAccessFileWriter output = new RandomAccessOutputStream(new File(filePath));
		fileSchema = new FileSchema(getJsonSchema());
		TSFileIOWriter tsfileWriter = new TSFileIOWriter(fileSchema, output);
		innerWriter = new TSRecordWriter(conf, tsfileWriter, writeSupport, fileSchema);
	}

	@After
	public void tearDown() throws Exception {
		file.delete();
	}

	@Test
	public void OneDeltaObjectTest() {
		String line = "";
		for (int i = 1; i <= 3; i++) {
			line = "root.car.d1," + i + ",s1,1,s2,1,s3,0.1,s4,0.1";
			TSRecord record = RecordUtils.parseSimpleTupleRecord(line, fileSchema);
			try {
				innerWriter.write(record);
			} catch (IOException | WriteProcessException e) {
				e.printStackTrace();
				fail(e.getMessage());
			}
		}
		assertEquals(0, innerWriter.query("root.car.d1", "s1").right.left.size());
		assertEquals(3, innerWriter.query("root.car.d1", "s1").left.length);
		for (int i = 1; i <= 3; i++) {
			DynamicOneColumnData columnData = innerWriter.query("root.car.d1", "s1").left;
			assertEquals(i, columnData.getTime(i - 1));
			assertEquals(1, columnData.getInt(i - 1));
		}
		for (int i = 4; i < 100; i++) {
			line = "root.car.d1," + i + ",s1,1,s2,1,s3,0.1,s4,0.1";
			TSRecord record = RecordUtils.parseSimpleTupleRecord(line, fileSchema);
			try {
				innerWriter.write(record);
			} catch (IOException | WriteProcessException e) {
				e.printStackTrace();
				fail(e.getMessage());
			}
		}
		assertEquals(innerWriter.query("root.car.d1", "s1").right.left.size(),
				innerWriter.query("root.car.d1", "s3").right.left.size());
		assertEquals(innerWriter.query("root.car.d1", "s2").right.left.size(),
				innerWriter.query("root.car.d1", "s4").right.left.size());
		assertEquals(null, innerWriter.query("root.car.d1", "s5").left);
		assertEquals(null, innerWriter.query("root.car.d1", "s5").right);
		assertEquals(null, innerWriter.query("root.car.d2", "s5").left);
		assertEquals(null, innerWriter.query("root.car.d2", "s5").right);
		try {
			innerWriter.close();
		} catch (IOException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void MultiDeltaObjectTest() {
		
		String line = "";
		for (int i = 1; i <= 3; i++) {
			line = "root.car.d1," + i + ",s1,1,s2,1,s3,0.1,s4,0.1";
			TSRecord record = RecordUtils.parseSimpleTupleRecord(line, fileSchema);
			try {
				innerWriter.write(record);
			} catch (IOException | WriteProcessException e) {
				e.printStackTrace();
				fail(e.getMessage());
			}
		}
		for (int i = 1; i <= 3; i++) {
			line = "root.car.d2," + i + ",s1,1,s2,1,s3,0.1,s4,0.1";
			TSRecord record = RecordUtils.parseSimpleTupleRecord(line, fileSchema);
			try {
				innerWriter.write(record);
			} catch (IOException | WriteProcessException e) {
				e.printStackTrace();
				fail(e.getMessage());
			}
		}
		for (int i = 1; i <= 3; i++) {
			DynamicOneColumnData columnData = innerWriter.query("root.car.d1", "s1").left;
			assertEquals(i, columnData.getTime(i - 1));
			assertEquals(1, columnData.getInt(i - 1));
		}
		for (int i = 1; i <= 3; i++) {
			DynamicOneColumnData columnData = innerWriter.query("root.car.d2", "s1").left;
			assertEquals(i, columnData.getTime(i - 1));
			assertEquals(1, columnData.getInt(i - 1));
		}
		
		for (int i = 4; i < 100; i++) {
			line = "root.car.d1," + i + ",s1,1,s2,1,s3,0.1,s4,0.1";
			TSRecord record = RecordUtils.parseSimpleTupleRecord(line, fileSchema);
			try {
				innerWriter.write(record);
			} catch (IOException | WriteProcessException e) {
				e.printStackTrace();
				fail(e.getMessage());
			}
		}
		
		for (int i = 4; i < 100; i++) {
			line = "root.car.d2," + i + ",s1,1,s2,1,s3,0.1,s4,0.1";
			TSRecord record = RecordUtils.parseSimpleTupleRecord(line, fileSchema);
			try {
				innerWriter.write(record);
			} catch (IOException | WriteProcessException e) {
				e.printStackTrace();
				fail(e.getMessage());
			}
		}
		
		assertEquals(innerWriter.query("root.car.d1", "s1").right.left.size(), innerWriter.query("root.car.d2", "s1").right.left.size());
		assertEquals(innerWriter.query("root.car.d1", "s1").left.length, innerWriter.query("root.car.d2", "s1").left.length);
		
		assertEquals(innerWriter.query("root.car.d1", "s2").right.left.size(), innerWriter.query("root.car.d2", "s2").right.left.size());
		assertEquals(innerWriter.query("root.car.d1", "s2").left.length, innerWriter.query("root.car.d2", "s2").left.length);
		
		assertEquals(innerWriter.query("root.car.d1", "s3").right.left.size(), innerWriter.query("root.car.d2", "s3").right.left.size());
		assertEquals(innerWriter.query("root.car.d1", "s3").left.length, innerWriter.query("root.car.d2", "s3").left.length);
		
		assertEquals(innerWriter.query("root.car.d1", "s4").right.left.size(), innerWriter.query("root.car.d2", "s4").right.left.size());
		assertEquals(innerWriter.query("root.car.d1", "s4").left.length, innerWriter.query("root.car.d2", "s4").left.length);
		
		assertEquals(null, innerWriter.query("root.car.d1", "s5").left);
		assertEquals(null, innerWriter.query("root.car.d1", "s5").right);
		assertEquals(null, innerWriter.query("root.car.d2", "s5").left);
		assertEquals(null, innerWriter.query("root.car.d2", "s5").right);
	}

	private static JSONObject getJsonSchema() {

		TSFileConfig conf = TSFileDescriptor.getInstance().getConfig();
		JSONObject s1 = new JSONObject();
		s1.put(JsonFormatConstant.MEASUREMENT_UID, "s1");
		s1.put(JsonFormatConstant.DATA_TYPE, TSDataType.INT32.toString());
		s1.put(JsonFormatConstant.MEASUREMENT_ENCODING, conf.defaultSeriesEncoder);

		JSONObject s2 = new JSONObject();
		s2.put(JsonFormatConstant.MEASUREMENT_UID, "s2");
		s2.put(JsonFormatConstant.DATA_TYPE, TSDataType.INT64.toString());
		s2.put(JsonFormatConstant.MEASUREMENT_ENCODING, conf.defaultSeriesEncoder);

		JSONObject s3 = new JSONObject();
		s3.put(JsonFormatConstant.MEASUREMENT_UID, "s3");
		s3.put(JsonFormatConstant.DATA_TYPE, TSDataType.FLOAT.toString());
		s3.put(JsonFormatConstant.MEASUREMENT_ENCODING, conf.defaultSeriesEncoder);

		JSONObject s4 = new JSONObject();
		s4.put(JsonFormatConstant.MEASUREMENT_UID, "s4");
		s4.put(JsonFormatConstant.DATA_TYPE, TSDataType.DOUBLE.toString());
		s4.put(JsonFormatConstant.MEASUREMENT_ENCODING, conf.defaultSeriesEncoder);

		JSONArray measureGroup = new JSONArray();
		measureGroup.put(s1);
		measureGroup.put(s2);
		measureGroup.put(s3);
		measureGroup.put(s4);

		JSONObject jsonSchema = new JSONObject();
		jsonSchema.put(JsonFormatConstant.DELTA_TYPE, "test_type");
		jsonSchema.put(JsonFormatConstant.JSON_SCHEMA, measureGroup);
		return jsonSchema;
	}
}

package cn.edu.thu.tsfile.timeseries.write;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.util.List;

import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import cn.edu.thu.tsfile.common.conf.TSFileConfig;
import cn.edu.thu.tsfile.common.conf.TSFileDescriptor;
import cn.edu.thu.tsfile.common.constant.JsonFormatConstant;
import cn.edu.thu.tsfile.common.utils.Pair;
import cn.edu.thu.tsfile.common.utils.RandomAccessOutputStream;
import cn.edu.thu.tsfile.common.utils.TSRandomAccessFileWriter;
import cn.edu.thu.tsfile.file.metadata.enums.CompressionTypeName;
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

	private int pageSize;
	private int RowGroupSize;
	private int pageCheckSizeThreshold;
	private int defaultMaxStringLength;
	private boolean cachePageData;
	@Before
	public void setUp() throws Exception {
		file.delete();
		pageSize = conf.pageSizeInByte;
		conf.pageSizeInByte = 200;
		RowGroupSize = conf.groupSizeInByte;
		conf.groupSizeInByte = 100000;
		pageCheckSizeThreshold = conf.pageCheckSizeThreshold;
		conf.pageCheckSizeThreshold = 1;
		defaultMaxStringLength = conf.maxStringLength;
		conf.maxStringLength = 2;
		cachePageData = conf.duplicateIncompletedPage;
		conf.duplicateIncompletedPage = true;
		TSRandomAccessFileWriter output = new RandomAccessOutputStream(new File(filePath));
		fileSchema = new FileSchema(getJsonSchema());
		TSFileIOWriter tsfileWriter = new TSFileIOWriter(fileSchema, output);
		innerWriter = new TSRecordWriter(conf, tsfileWriter, writeSupport, fileSchema);
	}

	@After
	public void tearDown() throws Exception {
		file.delete();
		conf.pageSizeInByte = pageSize;
		conf.groupSizeInByte = RowGroupSize;
		conf.pageCheckSizeThreshold = pageCheckSizeThreshold;
		conf.maxStringLength = defaultMaxStringLength;
		conf.duplicateIncompletedPage = cachePageData;
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
		List<Object> result = innerWriter.query("root.car.d1", "s1");
		DynamicOneColumnData left = (DynamicOneColumnData) result.get(0);
		Pair<List<ByteArrayInputStream>, CompressionTypeName> right = (Pair<List<ByteArrayInputStream>, CompressionTypeName>) result
				.get(1);
		assertEquals(0, right.left.size());
		assertEquals(3, left.valueLength);
		for (int i = 1; i <= 3; i++) {
			DynamicOneColumnData columnData = (DynamicOneColumnData) innerWriter.query("root.car.d1", "s1").get(0);
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

		result = innerWriter.query("root.car.d1", "s1");
		left = (DynamicOneColumnData) result.get(0);
		right = (Pair<List<ByteArrayInputStream>, CompressionTypeName>) result.get(1);

		DynamicOneColumnData left2 = (DynamicOneColumnData) innerWriter.query("root.car.d1", "s3").get(0);
		Pair<List<ByteArrayInputStream>, CompressionTypeName> right2 = (Pair<List<ByteArrayInputStream>, CompressionTypeName>) innerWriter
				.query("root.car.d1", "s3").get(1);
		assertEquals(right.left.size(), right2.left.size());

		right = (Pair<List<ByteArrayInputStream>, CompressionTypeName>) innerWriter.query("root.car.d1", "s2").get(1);
		right2 = (Pair<List<ByteArrayInputStream>, CompressionTypeName>) innerWriter.query("root.car.d1", "s4").get(1);
		assertEquals(right.left.size(), right2.left.size());

		assertEquals(null, innerWriter.query("root.car.d1", "s5").get(0));
		assertEquals(null, innerWriter.query("root.car.d1", "s5").get(1));
		assertEquals(null, innerWriter.query("root.car.d2", "s5").get(0));
		assertEquals(null, innerWriter.query("root.car.d2", "s5").get(1));
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
			DynamicOneColumnData columnData = (DynamicOneColumnData) innerWriter.query("root.car.d1", "s1").get(0);
			assertEquals(i, columnData.getTime(i - 1));
			assertEquals(1, columnData.getInt(i - 1));
		}
		for (int i = 1; i <= 3; i++) {
			DynamicOneColumnData columnData = (DynamicOneColumnData) innerWriter.query("root.car.d2", "s1").get(0);
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
		DynamicOneColumnData left = (DynamicOneColumnData) innerWriter.query("root.car.d1", "s1").get(0);
		DynamicOneColumnData left2 = (DynamicOneColumnData) innerWriter.query("root.car.d2", "s1").get(0);
		Pair<List<ByteArrayInputStream>, CompressionTypeName> right = (Pair<List<ByteArrayInputStream>, CompressionTypeName>) innerWriter
				.query("root.car.d1", "s1").get(1);
		Pair<List<ByteArrayInputStream>, CompressionTypeName> right2 = (Pair<List<ByteArrayInputStream>, CompressionTypeName>) innerWriter
				.query("root.car.d2", "s1").get(1);
		assertEquals(right.left.size(), right2.left.size());
		assertEquals(left.valueLength, left2.valueLength);

		right = (Pair<List<ByteArrayInputStream>, CompressionTypeName>) innerWriter.query("root.car.d1", "s2").get(1);
		right2 = (Pair<List<ByteArrayInputStream>, CompressionTypeName>) innerWriter.query("root.car.d2", "s2").get(1);
		assertEquals(right.left.size(), right2.left.size());

		left = (DynamicOneColumnData) innerWriter.query("root.car.d1", "s2").get(0);
		left2 = (DynamicOneColumnData) innerWriter.query("root.car.d2", "s2").get(0);
		assertEquals(left.valueLength, left2.valueLength);

		right = (Pair<List<ByteArrayInputStream>, CompressionTypeName>) innerWriter.query("root.car.d1", "s3").get(1);
		right2 = (Pair<List<ByteArrayInputStream>, CompressionTypeName>) innerWriter.query("root.car.d2", "s3").get(1);
		assertEquals(right.left.size(), right2.left.size());

		left = (DynamicOneColumnData) innerWriter.query("root.car.d1", "s3").get(0);
		left2 = (DynamicOneColumnData) innerWriter.query("root.car.d2", "s3").get(0);
		assertEquals(left.valueLength, left2.valueLength);

		right = (Pair<List<ByteArrayInputStream>, CompressionTypeName>) innerWriter.query("root.car.d1", "s4").get(1);
		right2 = (Pair<List<ByteArrayInputStream>, CompressionTypeName>) innerWriter.query("root.car.d2", "s4").get(1);

		assertEquals(right.left.size(), right2.left.size());

		left = (DynamicOneColumnData) innerWriter.query("root.car.d1", "s4").get(0);
		left2 = (DynamicOneColumnData) innerWriter.query("root.car.d2", "s4").get(0);

		assertEquals(left.valueLength, left2.valueLength);

		assertEquals(null, innerWriter.query("root.car.d1", "s5").get(0));
		assertEquals(null, innerWriter.query("root.car.d1", "s5").get(1));
		assertEquals(null, innerWriter.query("root.car.d2", "s5").get(0));
		assertEquals(null, innerWriter.query("root.car.d2", "s5").get(1));
	}

	private static JSONObject getJsonSchema() {

		TSFileConfig conf = TSFileDescriptor.getInstance().getConfig();
		JSONObject s1 = new JSONObject();
		s1.put(JsonFormatConstant.MEASUREMENT_UID, "s1");
		s1.put(JsonFormatConstant.DATA_TYPE, TSDataType.INT32.toString());
		s1.put(JsonFormatConstant.MEASUREMENT_ENCODING, conf.valueEncoder);

		JSONObject s2 = new JSONObject();
		s2.put(JsonFormatConstant.MEASUREMENT_UID, "s2");
		s2.put(JsonFormatConstant.DATA_TYPE, TSDataType.INT64.toString());
		s2.put(JsonFormatConstant.MEASUREMENT_ENCODING, conf.valueEncoder);

		JSONObject s3 = new JSONObject();
		s3.put(JsonFormatConstant.MEASUREMENT_UID, "s3");
		s3.put(JsonFormatConstant.DATA_TYPE, TSDataType.FLOAT.toString());
		s3.put(JsonFormatConstant.MEASUREMENT_ENCODING, conf.valueEncoder);

		JSONObject s4 = new JSONObject();
		s4.put(JsonFormatConstant.MEASUREMENT_UID, "s4");
		s4.put(JsonFormatConstant.DATA_TYPE, TSDataType.DOUBLE.toString());
		s4.put(JsonFormatConstant.MEASUREMENT_ENCODING, conf.valueEncoder);

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

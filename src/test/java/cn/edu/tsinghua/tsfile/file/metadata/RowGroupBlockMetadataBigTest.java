package cn.edu.tsinghua.tsfile.file.metadata;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

import cn.edu.tsinghua.tsfile.common.utils.TsRandomAccessFileWriter;
import cn.edu.tsinghua.tsfile.file.metadata.utils.TestHelper;
import cn.edu.tsinghua.tsfile.file.metadata.utils.Utils;
import cn.edu.tsinghua.tsfile.file.utils.ReadWriteToBytesUtils;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class RowGroupBlockMetadataBigTest {
	private static int deviceNum = 100;
	private static int sensorNum = 1000;
	private static String PATH = "target/test-big.ksn";
	public static final String DELTA_OBJECT_UID = "delta-3312";

	@Before
	public void setUp() throws Exception {
	}

	@After
	public void tearDown() throws Exception {
		File file = new File(PATH);
		if (file.exists())
			file.delete();
	}

	private static RowGroupMetaData createSimpleRowGroupMetaDataInTSF() throws UnsupportedEncodingException {
		RowGroupMetaData metaData = new RowGroupMetaData(RowGroupMetaDataTest.DELTA_OBJECT_UID,
				RowGroupMetaDataTest.MAX_NUM_ROWS, RowGroupMetaDataTest.TOTAL_BYTE_SIZE, new ArrayList<>(),
				RowGroupMetaDataTest.DELTA_OBJECT_TYPE);
		metaData.setPath(RowGroupMetaDataTest.FILE_PATH);
		for (int i = 0; i < sensorNum; i++) {
			metaData.addTimeSeriesChunkMetaData(TestHelper.createSimpleTimeSeriesChunkMetaDataInTSF());
		}
		return metaData;
	}

	@Test
	public void test() throws IOException {
		System.out.println("-------------Start Metadata big data test------------");
		long startTime = System.currentTimeMillis();
		List<RowGroupMetaData> rowGroupMetaDatas = new ArrayList<>();
		for (int i = 0; i < deviceNum; i++) {
			rowGroupMetaDatas.add(createSimpleRowGroupMetaDataInTSF());
		}
		TsRowGroupBlockMetaData metaData = new TsRowGroupBlockMetaData(rowGroupMetaDatas);
		metaData.setDeltaObjectID(DELTA_OBJECT_UID);
		System.out.println("1: create Metadata " + (System.currentTimeMillis() - startTime)+"ms");
		
		startTime = System.currentTimeMillis();
		File file = new File(PATH);
		if (file.exists())
			file.delete();
		TsRandomAccessFileWriter out = new TsRandomAccessFileWriter(file, "rw");
		BufferedOutputStream bufferedOutputStream = new BufferedOutputStream(out.getOutputStream());
		ReadWriteToBytesUtils.write(metaData, bufferedOutputStream);

		bufferedOutputStream.close();
		out.close();
		System.out.println("2: write to File" + (System.currentTimeMillis() - startTime)+"ms");

		FileInputStream fis = new FileInputStream(file);
		System.out.println("file size: " + fis.available());
		fis.close();
		
		FileInputStream fis2 = new FileInputStream(new File(PATH));
		startTime = System.currentTimeMillis();
		TsRowGroupBlockMetaData metaData2 = ReadWriteToBytesUtils.readTsRowGroupBlockMetaData(new BufferedInputStream(fis2));
		System.out.println("3: read from File" + (System.currentTimeMillis() - startTime)+"ms");
	    Utils.isRowGroupBlockMetadataEqual(metaData, metaData2);
	    System.out.println("-------------End Metadata big data test------------");
	}

}

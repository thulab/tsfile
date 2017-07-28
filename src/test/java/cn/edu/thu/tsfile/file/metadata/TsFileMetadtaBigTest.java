package cn.edu.thu.tsfile.file.metadata;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import cn.edu.thu.tsfile.common.utils.RandomAccessOutputStream;
import cn.edu.thu.tsfile.file.metadata.TSFileMetaData;
import cn.edu.thu.tsfile.file.metadata.converter.TSFileMetaDataConverter;
import cn.edu.thu.tsfile.file.metadata.utils.TestHelper;
import cn.edu.thu.tsfile.file.metadata.utils.Utils;
import cn.edu.thu.tsfile.file.utils.ReadWriteThriftFormatUtils;
import cn.edu.thu.tsfile.format.FileMetaData;

public class TsFileMetadtaBigTest {
	private static int deviceNum = 30000;
	private static int sensorNum = 10;
	private static String PATH = "target/test-big.ksn";
	private TSFileMetaDataConverter converter;

	@Before
	public void setUp() throws Exception {
		converter = new TSFileMetaDataConverter();
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
		System.out.println("-------------Start FileMetadata big data test------------");
		long startTime = System.currentTimeMillis();
		List<RowGroupMetaData> rowGroupMetaDatas = new ArrayList<>();
		for (int i = 0; i < deviceNum; i++) {
			rowGroupMetaDatas.add(createSimpleRowGroupMetaDataInTSF());
		}
		TSFileMetaData tsFileMetaData = new TSFileMetaData(rowGroupMetaDatas, new ArrayList<>(), 1);
		System.out.println("1: create Tsfile Metadata " + (System.currentTimeMillis() - startTime)+"ms");

		startTime = System.currentTimeMillis();
		FileMetaData fileMetaData = converter.toThriftFileMetadata(tsFileMetaData);
		System.out.println("2: covernet to Thrift " + (System.currentTimeMillis() - startTime)+"ms");

		Utils.isFileMetaDataEqual(tsFileMetaData, fileMetaData);
		
		startTime = System.currentTimeMillis();
		File file = new File(PATH);
		if (file.exists())
			file.delete();
		RandomAccessOutputStream out = new RandomAccessOutputStream(file, "rw");
		ReadWriteThriftFormatUtils.writeFileMetaData(fileMetaData, out);
		out.close();
		System.out.println("3: write to File" + (System.currentTimeMillis() - startTime)+"ms");

		FileInputStream fis = new FileInputStream(file);
		System.out.println("file size: " + fis.available());
		fis.close();
		
		FileInputStream fis2 = new FileInputStream(new File(PATH));

	    FileMetaData fileMetaData2 =
	        ReadWriteThriftFormatUtils.readFileMetaData(fis2);
	    Utils.isFileMetaDataEqual(tsFileMetaData, fileMetaData2);
	    System.out.println("-------------End FileMetadata big data test------------");
	}

	public static void main(String[] args) throws IOException {
		// long startTime = System.currentTimeMillis();
		// File file = new File(PATH);
		// RandomAccessOutputStream outputStream = new
		// RandomAccessOutputStream(file, "rw");
		// byte[] b = new byte[20*1024*1024];
		// outputStream.write(b);
		// outputStream.close();
		// System.out.println("3: "+(System.currentTimeMillis()-startTime));
		// FileInputStream fis = new FileInputStream(file);
		// System.out.println("file size: "+fis.available());
		// fis.close();
		//
		// if (file.exists())
		// file.delete();
	}

}

package cn.edu.tsinghua.tsfile.file.metadata;

import java.io.*;
import java.util.ArrayList;

import cn.edu.tsinghua.tsfile.file.utils.ReadWriteToBytesUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import cn.edu.tsinghua.tsfile.common.utils.TsRandomAccessFileWriter;
import cn.edu.tsinghua.tsfile.file.metadata.utils.TestHelper;
import cn.edu.tsinghua.tsfile.file.metadata.utils.Utils;
import cn.edu.tsinghua.tsfile.file.utils.ReadWriteThriftFormatUtils;
import cn.edu.tsinghua.tsfile.format.RowGroupBlockMetaData;

public class RowGroupBlockMetaDataTest {
	public static final String DELTA_OBJECT_UID = "delta-3312";
	final String PATH = "target/outputRowGroupBlock.ksn";
	final String BYTE_FILE_PATH = "src/test/resources/bytes.txt";

	@Before
	public void setUp() throws Exception {
	}

	@After
	public void tearDown() throws Exception {
		File file = new File(PATH);
		if (file.exists())
			file.delete();

		file = new File(BYTE_FILE_PATH);
		if (file.exists())
			file.delete();
	}

	@Test
	public void testWriteIntoFile() throws IOException {
		TsRowGroupBlockMetaData metaData = new TsRowGroupBlockMetaData();
		metaData.addRowGroupMetaData(TestHelper.createSimpleRowGroupMetaDataInTSF());
		metaData.addRowGroupMetaData(TestHelper.createSimpleRowGroupMetaDataInTSF());
		metaData.setDeltaObjectID(DELTA_OBJECT_UID);
		File file = new File(BYTE_FILE_PATH);
		if (file.exists())
			file.delete();
		FileOutputStream fos = new FileOutputStream(file);
		TsRandomAccessFileWriter out = new TsRandomAccessFileWriter(file, "rw");
		ReadWriteThriftFormatUtils.write(metaData.convertToThrift(), out.getOutputStream());

		out.close();
		fos.close();

		FileInputStream fis = new FileInputStream(new File(BYTE_FILE_PATH));
		Utils.isRowGroupBlockMetadataEqual(metaData, metaData.convertToThrift());

		Utils.isRowGroupBlockMetadataEqual(metaData,ReadWriteThriftFormatUtils.read(fis, new RowGroupBlockMetaData()));
	}

	@Test
	public void testWriteIntoFileByBytes() throws IOException {
		TsRowGroupBlockMetaData metaData = new TsRowGroupBlockMetaData();
		metaData.addRowGroupMetaData(TestHelper.createSimpleRowGroupMetaDataInTSF());
		metaData.addRowGroupMetaData(TestHelper.createSimpleRowGroupMetaDataInTSF());
		metaData.setDeltaObjectID(DELTA_OBJECT_UID);
		File file = new File(BYTE_FILE_PATH);
		if (file.exists())
			file.delete();
		TsRandomAccessFileWriter out = new TsRandomAccessFileWriter(file, "rw");
		BufferedOutputStream bufferedOutputStream = new BufferedOutputStream(out.getOutputStream());
		ReadWriteToBytesUtils.write(metaData, bufferedOutputStream);

		bufferedOutputStream.close();
		out.close();

		FileInputStream fis = new FileInputStream(new File(BYTE_FILE_PATH));
		TsRowGroupBlockMetaData metaData2 = ReadWriteToBytesUtils.readTsRowGroupBlockMetaData(new BufferedInputStream(fis));

		Utils.isRowGroupBlockMetadataEqual(metaData, metaData2);
	}

	@Test
	public void simpleTest() throws IOException {
		int looptime = 10000;
		long starttime, endtime;

		System.out.println("thrift:");
		starttime = System.currentTimeMillis();
		for(int i = 0;i < looptime;i++)
			testWriteIntoFile();
		endtime = System.currentTimeMillis();
		System.out.println(endtime - starttime);

		File file = new File(BYTE_FILE_PATH);
		if(file.exists()){
			System.out.println(file.length());
			file.delete();
		}

		System.out.println("bytes:");
		starttime = System.currentTimeMillis();
		for(int i = 0;i < looptime;i++)
			testWriteIntoFileByBytes();
		endtime = System.currentTimeMillis();
		System.out.println(endtime - starttime);

		file = new File(BYTE_FILE_PATH);
		if(file.exists())
			System.out.println(file.length());
	}

	@Test
	public void testConvertToThrift() throws UnsupportedEncodingException {
		TsRowGroupBlockMetaData metaData = new TsRowGroupBlockMetaData(null);
		metaData.setDeltaObjectID(DELTA_OBJECT_UID);
		Utils.isRowGroupBlockMetadataEqual(metaData, metaData.convertToThrift());
		metaData.setRowGroups(new ArrayList<>());
		Utils.isRowGroupBlockMetadataEqual(metaData, metaData.convertToThrift());
		metaData.addRowGroupMetaData(TestHelper.createSimpleRowGroupMetaDataInTSF());
		Utils.isRowGroupBlockMetadataEqual(metaData, metaData.convertToThrift());
		metaData.addRowGroupMetaData(TestHelper.createSimpleRowGroupMetaDataInTSF());
		Utils.isRowGroupBlockMetadataEqual(metaData, metaData.convertToThrift());
	}

	@Test
	public void testConvertToTSF() throws UnsupportedEncodingException {
		RowGroupBlockMetaData metaDataInThrift = new RowGroupBlockMetaData(new ArrayList<>());
		metaDataInThrift.setDelta_object_id(DELTA_OBJECT_UID);
		TsRowGroupBlockMetaData metaDataInTSF = new TsRowGroupBlockMetaData();
		metaDataInTSF.convertToTSF(metaDataInThrift);
		Utils.isRowGroupBlockMetadataEqual(metaDataInTSF, metaDataInTSF.convertToThrift());


//		metaDataInThrift.setRow_groups_metadata(new ArrayList<>());
//		metaDataInTSF.convertToTSF(metaDataInThrift);
//		Utils.isRowGroupBlockMetadataEqual(metaDataInTSF, metaDataInTSF.convertToThrift());

		metaDataInThrift.getRow_groups_metadata().add(TestHelper.createSimpleRowGroupMetaDataInThrift());
		metaDataInTSF.convertToTSF(metaDataInThrift);
		Utils.isRowGroupBlockMetadataEqual(metaDataInTSF, metaDataInTSF.convertToThrift());

		metaDataInThrift.getRow_groups_metadata().add(TestHelper.createSimpleRowGroupMetaDataInThrift());
		metaDataInTSF.convertToTSF(metaDataInThrift);
		Utils.isRowGroupBlockMetadataEqual(metaDataInTSF, metaDataInTSF.convertToThrift());
	}

}

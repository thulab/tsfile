package cn.edu.tsinghua.tsfile.file.metadata;

import java.io.*;

import cn.edu.tsinghua.tsfile.file.utils.ReadWriteToBytesUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import cn.edu.tsinghua.tsfile.common.utils.TsRandomAccessFileWriter;
import cn.edu.tsinghua.tsfile.file.metadata.utils.TestHelper;
import cn.edu.tsinghua.tsfile.file.metadata.utils.Utils;

public class RowGroupBlockMetaDataTest {
	public static final String DELTA_OBJECT_UID = "delta-3312";
	final String PATH = "target/outputRowGroupBlock.ksn";

	@Before
	public void setUp() throws Exception {
	}

	@After
	public void tearDown() throws Exception {
		File file = new File(PATH);
		if (file.exists())
			file.delete();
	}

	@Test
	public void testWriteIntoFileByBytes() throws IOException {
		TsRowGroupBlockMetaData metaData = new TsRowGroupBlockMetaData();
		metaData.addRowGroupMetaData(TestHelper.createSimpleRowGroupMetaDataInTSF());
		metaData.addRowGroupMetaData(TestHelper.createSimpleRowGroupMetaDataInTSF());
		metaData.setDeltaObjectID(DELTA_OBJECT_UID);
		File file = new File(PATH);
		if (file.exists())
			file.delete();
		TsRandomAccessFileWriter out = new TsRandomAccessFileWriter(file, "rw");
		BufferedOutputStream bufferedOutputStream = new BufferedOutputStream(out.getOutputStream());
		ReadWriteToBytesUtils.write(metaData, bufferedOutputStream);

		bufferedOutputStream.close();
		out.close();

		FileInputStream fis = new FileInputStream(new File(PATH));
		TsRowGroupBlockMetaData metaData2 = ReadWriteToBytesUtils.readTsRowGroupBlockMetaData(new BufferedInputStream(fis));

		Utils.isRowGroupBlockMetadataEqual(metaData, metaData2);
	}
}

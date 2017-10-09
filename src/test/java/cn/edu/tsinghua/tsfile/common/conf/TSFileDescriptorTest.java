package cn.edu.tsinghua.tsfile.common.conf;

import static org.junit.Assert.*;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import cn.edu.tsinghua.tsfile.common.constant.SystemConstant;

public class TSFileDescriptorTest {

	@Before
	public void setUp() throws Exception {
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void test() {
		System.setProperty(SystemConstant.TSFILE_CONF, "src/test/resources/tsfile-format.properties.test");
		TSFileConfig config = TSFileDescriptor.getInstance().getConfig();
		assertEquals(config.groupSizeInByte, 134217729);
		assertEquals(config.pageSizeInByte, 1048577);
		assertEquals(config.maxNumberOfPointsInPage, 1048577);
		assertEquals(config.timeSeriesDataType, "INT32");
		assertEquals(config.maxStringLength, 129);
		assertEquals(config.floatPrecision, 3);
		assertEquals(config.timeSeriesEncoder, "RLE");
		assertEquals(config.valueEncoder, "RLE");
		assertEquals(config.compressor, "SNAPPY");
	}

}

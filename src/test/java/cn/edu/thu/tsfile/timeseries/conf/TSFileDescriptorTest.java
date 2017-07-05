package cn.edu.thu.tsfile.timeseries.conf;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import cn.edu.thu.tsfile.common.conf.TSFileConfig;
import cn.edu.thu.tsfile.common.conf.TSFileDescriptor;
import static org.junit.Assert.assertEquals;
/**
 * Note that this test case should run separately.
 * @author XuYi
 */
public class TSFileDescriptorTest {
//    public int rowGroupSizePre = 128 * 1024 * 1024;
//    public int pageSizePre = 1024 * 1024;
//    public int maxPointNumberInPagePre = 1024 * 1024;
//    public String timeDataTypePre = "INT64";
//    public int maxStringLengthPre = 128;
//    public int floatPrecisionPre = 2;
//    public String timeSeriesEncoderPre = "TS_2DIFF";
//    public String valueSeriesEncoderPre = "RLE";
//    public String compressorPre = "UNCOMPRESSED";
//    public TSFileConfig config;
    
    @Before
    public void before() {
//	config = TSFileDescriptor.getInstance().getConfig();
    }

    @After
    public void after() {
//	config.rowGroupSize = rowGroupSizePre;
//	config.pageSize = pageSizePre;
//	config.maxPointNumberInPage = maxPointNumberInPagePre;
//	config.timeDataType = timeDataTypePre;
//	config.maxStringLength = maxStringLengthPre;
//	config.floatPrecision = floatPrecisionPre;
//	config.timeSeriesEncoder = timeSeriesEncoderPre;
//	config.valueSeriesEncoder = valueSeriesEncoderPre;
//	config.compressor = compressorPre;
    }

    @Test
    public void testLoadProp() {
//	assertEquals(config.rowGroupSize, 123456789);
//	assertEquals(config.pageSize, 123456);
//	assertEquals(config.maxPointNumberInPage, 12345);
//	assertEquals(config.timeDataType, "INT32");
//	assertEquals(config.maxStringLength, 64);
//	assertEquals(config.floatPrecision, 5);
//	assertEquals(config.timeSeriesEncoder, "RLE");
//	assertEquals(config.valueSeriesEncoder, "PLAIN");
//	assertEquals(config.compressor, "SNAPPY");
    }

}

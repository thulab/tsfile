package cn.edu.tsinghua.tsfile.timeseries.write.schema.converter;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.junit.Test;

import cn.edu.tsinghua.tsfile.common.constant.JsonFormatConstant;
import cn.edu.tsinghua.tsfile.common.exception.metadata.MetadataArgsErrorException;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSEncoding;

/**
 * 
 * @author kangrong
 *
 */
public class TSEncodingConverterTest {
    private String noExists = "no_exists";
    private String intStr1 = "1";
    private Integer int1 = 1;
    private String errIntStr1 = "lqwk";


//    @Test
//    public void testCheckParameterRLE() {
//        TSEncoding encode = TSEncoding.RLE;
//        try {
//            assertEquals(int1,
//                    TSEncodingBuilder.checkParameter(encode, JsonFormatConstant.MAX_POINT_NUMBER, intStr1));
//        } catch (MetadataArgsErrorException e1) {
//            assertTrue(false);
//        }
//        try {
//            TSEncodingBuilder.checkParameter(encode, JsonFormatConstant.MAX_POINT_NUMBER, errIntStr1);
//            fail();
//        } catch (Exception e) {
//            assertTrue(e instanceof MetadataArgsErrorException);
//            assertEquals("paramter max_point_number meets error integer format :lqwk", e.getMessage());
//        }
//    }
//
//    @Test
//    public void testCheckParameterTS_2DIFF() {
//        TSEncoding encode = TSEncoding.TS_2DIFF;
//        try {
//            assertEquals(int1,
//                    TSEncodingBuilder.checkParameter(encode, JsonFormatConstant.MAX_POINT_NUMBER, intStr1));
//        } catch (MetadataArgsErrorException e1) {
//            assertTrue(false);
//        }
//        try {
//            TSEncodingBuilder.checkParameter(encode, JsonFormatConstant.MAX_POINT_NUMBER, errIntStr1);
//            fail();
//        } catch (Exception e) {
//            assertTrue(e instanceof MetadataArgsErrorException);
//            assertEquals("paramter max_point_number meets error integer format :lqwk", e.getMessage());
//        }
//    }
}
